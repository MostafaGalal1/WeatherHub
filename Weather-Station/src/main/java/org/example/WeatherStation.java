package org.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.example.model.WeatherData;
import org.example.model.WeatherMessage;

import java.util.concurrent.*;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

public class WeatherStation {
    private final Long stationID;
    private final AtomicLong ID_GENERATOR;
    private final ScheduledExecutorService SCHEDULER;
    private final Random RANDOM;
    private final ObjectMapper objectMapper;

    private KafkaProducer<String, String> producer;

    public WeatherStation() {
        this.stationID = getStationID();
        this.SCHEDULER = Executors.newScheduledThreadPool(1);
        this.ID_GENERATOR = new AtomicLong(0);
        this.RANDOM = new Random();
        this.objectMapper = new ObjectMapper();
    }

    public void emit() {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");

        this.producer = new KafkaProducer<>(props);

        this.SCHEDULER.scheduleAtFixedRate(() -> {
            String message = getMessage();
            if (message != null) {
                ProducerRecord<String, String> record = new ProducerRecord<>("Weather-Metrics", message);
                this.producer.send(record);
                System.out.println("Message sent: " + message);
            }
        }, 0, 1, TimeUnit.SECONDS);
    }

    private Long getStationID(){
        String stationName = System.getenv("stationName");

        if (stationName != null && stationName.contains("-")) {
            String[] parts = stationName.split("-");
            try {
                return Long.parseLong(parts[parts.length - 1]);
            } catch (NumberFormatException e) {
                System.err.println("Invalid station number format: " + stationName);
            }
        }

        return null;
    }

    private String getMessage() {
        // 10% for dropping message
        Long messageID = this.ID_GENERATOR.incrementAndGet();

        if (this.RANDOM.nextDouble() < 0.10) {
            return null;
        }

        // 30% for LOW, 40% for MEDIUM, 30% for HIGH
        double p = RANDOM.nextDouble();
        String batteryStatus =
            p < 0.3 ? BatteryStatus.LOW.name() :
            p < 0.7 ? BatteryStatus.MEDIUM.name() :
                      BatteryStatus.HIGH.name();

        long timestamp = System.currentTimeMillis();

        int humidity    = RANDOM.nextInt(100);
        int temperature = 100;
        int windSpeed   = 13;

        WeatherData weatherData = new WeatherData(humidity, temperature, windSpeed);
        WeatherMessage weatherMessage = new WeatherMessage(this.stationID, messageID, batteryStatus, timestamp, weatherData);

        try {
            return this.objectMapper.writeValueAsString(weatherMessage);
        } catch (Exception e) {
            System.err.println("Error serializing message: " + e.getMessage());
            return null;
        }
    }

    public static void main(String[] args) {
        WeatherStation weatherStation = new WeatherStation();
        weatherStation.emit();
    }
}