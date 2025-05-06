package org.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.example.model.RainMessage;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class RainProcessor {
    private final ObjectMapper objectMapper;

    public RainProcessor() {
        this.objectMapper = new ObjectMapper();
    }

    public void detect (){
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "raining-processor");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, System.getenv().getOrDefault("BOOTSTRAP_SERVERS", "kafka:9092"));
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");

        StreamsBuilder builder = new StreamsBuilder();
        builder.stream("Weather-Metrics")
                .filter((key, value) -> {
                    try { return parseHumidity(value) > 70; }
                    catch (Exception e) { return false; }
                })
                .mapValues(value -> {
                    try {
                        Long stationID = parseStationID(value);
                        Integer humidity = parseHumidity(value);
                        RainMessage message = new RainMessage(stationID, humidity);
                        return this.objectMapper.writeValueAsString(message);
                    } catch (Exception e) {
                        System.err.println("Error parsing value: " + e.getMessage());
                        return null;
                    }
                })
                .to("Raining");

        try (KafkaStreams streams = new KafkaStreams(builder.build(), props)) {
            CountDownLatch latch = new CountDownLatch(1);

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                streams.close();
                latch.countDown();
            }));

            try {
                streams.start();
                latch.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private Long parseStationID(Object record) {
        try {
            JsonNode root = objectMapper.readTree(record.toString());
            return root.path("station_id").asLong();
        } catch (Exception e) {
            System.err.println("Error parsing station ID: " + e.getMessage());
            return 0L;
        }
    }

    private Integer parseHumidity(Object record) {
        try {
            JsonNode root = objectMapper.readTree(record.toString());
            return root.path("weather").path("humidity").asInt();
        } catch (Exception e) {
            System.err.println("Error parsing humidity: " + e.getMessage());
            return 0;
        }
    }

    public static void main(String[] args) {
        RainProcessor rainingProcessor = new RainProcessor();
        rainingProcessor.detect();
    }
}