package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.Properties;
import java.util.Random;

public class WeatherStation {
    private final Long stationNo;
    private final AtomicInteger ID_GENERATOR;
    private final ScheduledExecutorService SCHEDULER;
    private final Random RANDOM;

    public WeatherStation() {
        this.stationNo = getStationNo();
        this.SCHEDULER = Executors.newScheduledThreadPool(1);
        this.ID_GENERATOR = new AtomicInteger(0);
        this.RANDOM = new Random();
    }

    private Long getStationNo(){
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

    public void emit() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "KAFKA:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        this.SCHEDULER.scheduleAtFixedRate(() -> {
            try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
                String message = getMessage();
                if (message != null) {
                    ProducerRecord<String, String> record = new ProducerRecord<>("Weather_Metrics", message);
                    producer.send(record);
                    System.out.println("Message sent: " + message);
                }
            }
        }, 0, 30, TimeUnit.SECONDS);
    }

    public String getMessage() {
        // 10% for dropping message
        if (this.RANDOM.nextDouble() < 0.10) {
            return null;
        }

        int messageID = this.ID_GENERATOR.incrementAndGet();

        // 30% for LOW, 40% for MEDIUM, 30% for HIGH
        double p = RANDOM.nextDouble();
        String batteryStatus =
            p < 0.3 ? BatteryStatus.LOW.name() :
            p < 0.7 ? BatteryStatus.MEDIUM.name() :
                      BatteryStatus.HIGH.name();

        long timestamp = System.currentTimeMillis();

        int humidity    = 35;
        int temperature = 100;
        int windSpeed   = 13;

        return String.format(
            "{"
            +   "\"station_id\": %d,"
            +   "\"s_no\": %d,"
            +   "\"battery_status\": \"%s\","
            +   "\"status_timestamp\": %d,"
            +   "\"weather\": {"
            +     "\"humidity\": %d,"
            +     "\"temperature\": %d,"
            +     "\"wind_speed\": %d"
            +   "}"
            + "}",
            stationNo, messageID, batteryStatus, timestamp, humidity, temperature, windSpeed
        );
    }

    public static void main(String[] args) {
        WeatherStation weatherStation = new WeatherStation();
        weatherStation.emit();
    }
}