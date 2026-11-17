package com.yprnv7.kstreamlake;

import com.yprnv7.kstreamlake.config.KafkaConfig;
import com.yprnv7.kstreamlake.stream.StreamTopologyBuilder;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class KStreamLakeApp {

    private static final Logger log = LoggerFactory.getLogger(KStreamLakeApp.class);

    public static void main(String[] args) {
        String bootstrapServers = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
        String applicationId = System.getenv().getOrDefault("KAFKA_APP_ID", "kstream-lake-engine");

        log.info("Starting KStream-Lake Pipeline with bootstrapServers={}, appId={}", bootstrapServers, applicationId);

        Properties props = KafkaConfig.buildStreamsProperties(bootstrapServers, applicationId);
        Topology topology = StreamTopologyBuilder.buildTopology(Duration.ofMinutes(5), Duration.ofSeconds(30));

        KafkaStreams streams = new KafkaStreams(topology, props);
        final CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Initiating graceful shutdown for KStream-Lake...");
            streams.close(Duration.ofSeconds(10));
            latch.countDown();
        }));

        try {
            streams.start();
            log.info("KStream-Lake topology is running successfully.");
            latch.await();
        } catch (InterruptedException e) {
            log.warn("KStream-Lake interrupted: {}", e.getMessage());
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            log.error("Fatal error in KStream-Lake engine: {}", e.getMessage(), e);
            System.exit(1);
        }
    }
}
