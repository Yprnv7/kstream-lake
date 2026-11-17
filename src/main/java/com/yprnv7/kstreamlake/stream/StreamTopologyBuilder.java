package com.yprnv7.kstreamlake.stream;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.yprnv7.kstreamlake.model.DeadLetterRecord;
import com.yprnv7.kstreamlake.model.TransactionEvent;
import com.yprnv7.kstreamlake.model.UserWindowAggregation;
import com.yprnv7.kstreamlake.serde.JsonSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;

public class StreamTopologyBuilder {

    public static final String INPUT_TOPIC = "raw-transactions";
    public static final String AGGREGATE_OUTPUT_TOPIC = "user-aggregates";
    public static final String LAKE_SINK_TOPIC = "lake-clean-events";
    public static final String DLQ_TOPIC = "dead-letter-queue";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static Topology buildTopology(Duration windowDuration, Duration gracePeriod) {
        StreamsBuilder builder = new StreamsBuilder();

        JsonSerde<TransactionEvent> txSerde = new JsonSerde<>(TransactionEvent.class);
        JsonSerde<UserWindowAggregation> aggSerde = new JsonSerde<>(UserWindowAggregation.class);
        JsonSerde<DeadLetterRecord> dlqSerde = new JsonSerde<>(DeadLetterRecord.class);

        // 1. Ingest raw stream as string
        KStream<String, String> rawSource = builder.stream(INPUT_TOPIC, Consumed.with(Serdes.String(), Serdes.String()));

        // 2. Parse and filter valid transaction stream
        KStream<String, TransactionEvent> validStream = rawSource
                .filter((key, rawJson) -> {
                    try {
                        TransactionEvent event = MAPPER.readValue(rawJson, TransactionEvent.class);
                        return event != null && event.isValid();
                    } catch (Exception e) {
                        return false;
                    }
                })
                .map((key, rawJson) -> {
                    try {
                        TransactionEvent event = MAPPER.readValue(rawJson, TransactionEvent.class);
                        return new KeyValue<>(event.getUserId(), event);
                    } catch (Exception e) {
                        return new KeyValue<>(key != null ? key : "unknown", null);
                    }
                });

        // 3. Route invalid/malformed records to Dead-Letter Queue (DLQ)
        rawSource
                .filter((key, rawJson) -> {
                    try {
                        TransactionEvent event = MAPPER.readValue(rawJson, TransactionEvent.class);
                        return event == null || !event.isValid();
                    } catch (Exception e) {
                        return true;
                    }
                })
                .mapValues(rawJson -> new DeadLetterRecord(rawJson, "Schema validation or JSON parsing failed", System.currentTimeMillis()))
                .to(DLQ_TOPIC, Produced.with(Serdes.String(), dlqSerde));

        // 4. Clean stream sink for columnar lakehouse
        validStream.to(LAKE_SINK_TOPIC, Produced.with(Serdes.String(), txSerde));

        // 5. Stateful Windowed Aggregation per User
        TimeWindows timeWindows = TimeWindows.ofSizeWithNoGrace(windowDuration);

        validStream
                .groupByKey(Grouped.with(Serdes.String(), txSerde))
                .windowedBy(timeWindows)
                .aggregate(
                        UserWindowAggregation::new,
                        (userId, tx, agg) -> {
                            agg.setUserId(userId);
                            agg.setTransactionCount(agg.getTransactionCount() + 1);
                            agg.setTotalVolume(agg.getTotalVolume() + tx.getAmount());
                            agg.setMaxTransactionAmount(Math.max(agg.getMaxTransactionAmount(), tx.getAmount()));
                            agg.setAverageAmount(agg.getTotalVolume() / agg.getTransactionCount());
                            if (tx.getAmount() > 10000.0 || agg.getTransactionCount() > 50) {
                                agg.setAnomalyFlag(true);
                            }
                            return agg;
                        },
                        Materialized.with(Serdes.String(), aggSerde)
                )
                .toStream()
                .map((Windowed<String> windowedKey, UserWindowAggregation agg) -> {
                    agg.setWindowStart(windowedKey.window().start());
                    agg.setWindowEnd(windowedKey.window().end());
                    return new KeyValue<>(windowedKey.key(), agg);
                })
                .to(AGGREGATE_OUTPUT_TOPIC, Produced.with(Serdes.String(), aggSerde));

        return builder.build();
    }
}
