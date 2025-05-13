package org.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.example.model.DeadLetterMessage;
import org.example.model.WeatherMessage;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ExpiryProcessor {
    private static final long TTL_MS = Duration.ofMinutes(1).toMillis();
    private final ObjectMapper objectMapper = new ObjectMapper();

    public void detect() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "expiry-processor");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, System.getenv().getOrDefault("BOOTSTRAP_SERVERS", "kafka:9092"));
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");

        StreamsBuilder builder = new StreamsBuilder();

        builder.stream("Weather-Metrics")
                .filter((key, value) -> {
                    try {
                        long timestamp = parseTimestamp(value);
                        return System.currentTimeMillis() - timestamp > TTL_MS;
                    } catch (Exception e) { return true; }
                })
                .mapValues(value -> {
                    try {
                        DeadLetterMessage message = new DeadLetterMessage("Expired", System.currentTimeMillis(), (WeatherMessage) value);
                        return this.objectMapper.writeValueAsString(message);
                    } catch (Exception e) {
                        System.err.println("Error parsing value: " + e.getMessage());
                        return null;
                    }
                })
                .to("Weather-Metrics-DLT");

        try(KafkaStreams streams = new KafkaStreams(builder.build(), props)) {
            CountDownLatch latch = new CountDownLatch(1);

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                streams.close(Duration.ofSeconds(10));
                latch.countDown();
            }));

            streams.start();
            try {
                latch.await();
            } catch (InterruptedException ignored) {
            }
        }
    }

    private Integer parseTimestamp(Object record) {
        try {
            JsonNode root = this.objectMapper.readTree(record.toString());
            return root.path("weather").path("humidity").asInt();
        } catch (Exception e) {
            System.err.println("Error parsing humidity: " + e.getMessage());
            return 0;
        }
    }

    public static void main(String[] args) {
        new ExpiryProcessor().detect();
    }
}
