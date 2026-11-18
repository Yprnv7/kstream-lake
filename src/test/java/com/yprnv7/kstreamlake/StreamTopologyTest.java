package com.yprnv7.kstreamlake;

import com.yprnv7.kstreamlake.model.DeadLetterRecord;
import com.yprnv7.kstreamlake.model.TransactionEvent;
import com.yprnv7.kstreamlake.model.UserWindowAggregation;
import com.yprnv7.kstreamlake.serde.JsonSerde;
import com.yprnv7.kstreamlake.stream.StreamTopologyBuilder;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

public class StreamTopologyTest {

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, String> inputTopic;
    private TestOutputTopic<String, UserWindowAggregation> aggregateOutputTopic;
    private TestOutputTopic<String, TransactionEvent> lakeOutputTopic;
    private TestOutputTopic<String, DeadLetterRecord> dlqOutputTopic;

    @BeforeEach
    public void setup() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-kstream-lake");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        Topology topology = StreamTopologyBuilder.buildTopology(Duration.ofMinutes(1), Duration.ZERO);
        testDriver = new TopologyTestDriver(topology, props);

        JsonSerde<TransactionEvent> txSerde = new JsonSerde<>(TransactionEvent.class);
        JsonSerde<UserWindowAggregation> aggSerde = new JsonSerde<>(UserWindowAggregation.class);
        JsonSerde<DeadLetterRecord> dlqSerde = new JsonSerde<>(DeadLetterRecord.class);

        inputTopic = testDriver.createInputTopic(
                StreamTopologyBuilder.INPUT_TOPIC,
                Serdes.String().serializer(),
                Serdes.String().serializer()
        );

        aggregateOutputTopic = testDriver.createOutputTopic(
                StreamTopologyBuilder.AGGREGATE_OUTPUT_TOPIC,
                Serdes.String().deserializer(),
                aggSerde.deserializer()
        );

        lakeOutputTopic = testDriver.createOutputTopic(
                StreamTopologyBuilder.LAKE_SINK_TOPIC,
                Serdes.String().deserializer(),
                txSerde.deserializer()
        );

        dlqOutputTopic = testDriver.createOutputTopic(
                StreamTopologyBuilder.DLQ_TOPIC,
                Serdes.String().deserializer(),
                dlqSerde.deserializer()
        );
    }

    @AfterEach
    public void tearDown() {
        if (testDriver != null) {
            testDriver.close();
        }
    }

    @Test
    @DisplayName("Valid transaction events are forwarded to lake-sink and aggregated in window")
    public void testValidTransactionStreamProcessing() {
        long now = Instant.now().toEpochMilli();
        String json1 = String.format(
                "{\"transaction_id\":\"tx-100\",\"user_id\":\"usr-01\",\"merchant_id\":\"m-9\",\"amount\":250.0,\"currency\":\"EUR\",\"country\":\"IE\",\"timestamp\":%d}",
                now
        );
        String json2 = String.format(
                "{\"transaction_id\":\"tx-101\",\"user_id\":\"usr-01\",\"merchant_id\":\"m-9\",\"amount\":350.0,\"currency\":\"EUR\",\"country\":\"IE\",\"timestamp\":%d}",
                now + 1000
        );

        inputTopic.pipeInput("usr-01", json1);
        inputTopic.pipeInput("usr-01", json2);

        // Verify clean events emitted to Lake topic
        List<TransactionEvent> lakeEvents = lakeOutputTopic.readValuesToList();
        assertEquals(2, lakeEvents.size());
        assertEquals("tx-100", lakeEvents.get(0).getTransactionId());
        assertEquals("tx-101", lakeEvents.get(1).getTransactionId());

        // Verify aggregation window
        List<UserWindowAggregation> aggregates = aggregateOutputTopic.readValuesToList();
        assertFalse(aggregates.isEmpty());

        UserWindowAggregation latest = aggregates.get(aggregates.size() - 1);
        assertEquals("usr-01", latest.getUserId());
        assertEquals(2, latest.getTransactionCount());
        assertEquals(600.0, latest.getTotalVolume(), 0.001);
        assertEquals(350.0, latest.getMaxTransactionAmount(), 0.001);
        assertEquals(300.0, latest.getAverageAmount(), 0.001);
        assertFalse(latest.isAnomalyFlag());
    }

    @Test
    @DisplayName("Invalid or corrupt payloads are filtered and routed to Dead-Letter Queue (DLQ)")
    public void testDeadLetterQueueRouting() {
        // Malformed JSON
        inputTopic.pipeInput("bad-key", "{bad-json-unclosed");

        // Missing required fields (amount <= 0)
        inputTopic.pipeInput("usr-02", "{\"transaction_id\":\"tx-bad\",\"user_id\":\"usr-02\",\"amount\":-5.0,\"currency\":\"EUR\",\"timestamp\":12345}");

        List<DeadLetterRecord> dlqRecords = dlqOutputTopic.readValuesToList();
        assertEquals(2, dlqRecords.size());
        assertTrue(dlqRecords.get(0).getRawPayload().contains("{bad-json-unclosed"));
        assertTrue(dlqRecords.get(1).getRawPayload().contains("tx-bad"));

        // Ensure no corrupt events made it to clean lake sink
        assertTrue(lakeOutputTopic.isEmpty());
    }

    @Test
    @DisplayName("Large volume or single high-value transaction triggers anomaly flag")
    public void testAnomalyDetection() {
        long now = Instant.now().toEpochMilli();
        String highValueTx = String.format(
                "{\"transaction_id\":\"tx-whale\",\"user_id\":\"usr-vip\",\"merchant_id\":\"m-luxury\",\"amount\":15000.0,\"currency\":\"EUR\",\"country\":\"IE\",\"timestamp\":%d}",
                now
        );

        inputTopic.pipeInput("usr-vip", highValueTx);

        List<UserWindowAggregation> aggregates = aggregateOutputTopic.readValuesToList();
        assertFalse(aggregates.isEmpty());
        UserWindowAggregation agg = aggregates.get(aggregates.size() - 1);
        assertTrue(agg.isAnomalyFlag());
    }

    @Test
    @DisplayName("Rolling Z-score anomaly detector flags transactions > 3 standard deviations above window mean")
    public void testZScoreStatisticalAnomalyDetection() {
        long now = Instant.now().toEpochMilli();
        String userId = "usr-stat-01";

        // Feed baseline transactions with small natural variance around 20.0
        double[] baselineAmounts = {20.0, 22.0, 19.0, 21.0, 20.5, 19.5, 20.0, 21.5, 19.0, 20.0};
        for (int i = 0; i < baselineAmounts.length; i++) {
            String json = String.format(
                    "{\"transaction_id\":\"tx-base-%d\",\"user_id\":\"%s\",\"merchant_id\":\"m-groceries\",\"amount\":%.2f,\"currency\":\"EUR\",\"country\":\"IE\",\"timestamp\":%d}",
                    i, userId, baselineAmounts[i], now + (i * 100)
            );
            inputTopic.pipeInput(userId, json);
        }

        // Verify baseline has no anomaly flag
        List<UserWindowAggregation> baselineAggs = aggregateOutputTopic.readValuesToList();
        assertFalse(baselineAggs.isEmpty());
        UserWindowAggregation lastBaseline = baselineAggs.get(baselineAggs.size() - 1);
        assertFalse(lastBaseline.isAnomalyFlag(), "Baseline transactions should not trigger anomaly");

        // Inject sudden spike > 3 standard deviations (e.g. 150.0 vs mean ~20 with stdDev ~1)
        String spikeJson = String.format(
                "{\"transaction_id\":\"tx-spike\",\"user_id\":\"%s\",\"merchant_id\":\"m-electronics\",\"amount\":150.0,\"currency\":\"EUR\",\"country\":\"IE\",\"timestamp\":%d}",
                userId, now + 2000
        );
        inputTopic.pipeInput(userId, spikeJson);

        List<UserWindowAggregation> afterSpikeAggs = aggregateOutputTopic.readValuesToList();
        assertFalse(afterSpikeAggs.isEmpty());
        UserWindowAggregation spikeAgg = afterSpikeAggs.get(afterSpikeAggs.size() - 1);

        assertTrue(spikeAgg.isAnomalyFlag(), "Spike > 3 std devs must trigger anomalyFlag = true");
        assertTrue(spikeAgg.getZScore() >= 3.0, "Z-score must be >= 3.0, actual: " + spikeAgg.getZScore());
    }
}
