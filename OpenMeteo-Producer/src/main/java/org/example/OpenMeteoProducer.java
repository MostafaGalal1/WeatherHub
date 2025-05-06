package org.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.example.model.WeatherData;
import org.example.model.WeatherMessage;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Random;
import java.util.concurrent.*;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

public class OpenMeteoProducer {
    private final HttpClient client;
    private final AtomicLong ID_GENERATOR;
    private final ScheduledExecutorService SCHEDULER;
    private final Random RANDOM;
    private final ObjectMapper objectMapper;

    public OpenMeteoProducer() {
        this.client = HttpClient.newHttpClient();
        this.SCHEDULER = Executors.newScheduledThreadPool(1);
        this.ID_GENERATOR = new AtomicLong(0);
        this.RANDOM = new Random();
        this.objectMapper = new ObjectMapper();
    }

    public void emit(){
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");

        this.SCHEDULER.scheduleAtFixedRate(() -> {
            try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
                String message = getMessage();
                if (message != null) {
                    ProducerRecord<String, String> record = new ProducerRecord<>("Weather-Metrics", message);
                    producer.send(record);
                    System.out.println("Message sent: " + message);
                }
            }
        }, 0, 10, TimeUnit.SECONDS);
    }

    private String getMessage() {
        try {
            String OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast?latitude=31.2001&longitude=29.9187&current=relative_humidity_2m,temperature_2m,wind_speed_10m&timezone=Africa%2FCairo&forecast_days=16";
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(OPEN_METEO_URL))
                .timeout(Duration.ofSeconds(10))
                .GET()
                .build();

            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 200) {
                Long messageID =  this.ID_GENERATOR.incrementAndGet();

                double p = RANDOM.nextDouble();
                String batteryStatus =
                        p < 0.3 ? BatteryStatus.LOW.name() :
                                p < 0.7 ? BatteryStatus.MEDIUM.name() :
                                        BatteryStatus.HIGH.name();

                long timestamp = System.currentTimeMillis();

                JsonNode root = objectMapper.readTree(response.body());
                JsonNode current = root.path("current");

                int humidity = current.path("relative_humidity_2m").asInt();
                int temperature = (int) Math.round(current.path("temperature_2m").asDouble());
                int windSpeed = (int) Math.round(current.path("wind_speed_10m").asDouble());

                WeatherData weatherData = new WeatherData(humidity, temperature, windSpeed);
                WeatherMessage weatherMessage = new WeatherMessage(10L, messageID, batteryStatus, timestamp, weatherData);
                return objectMapper.writeValueAsString(weatherMessage);
            } else {
                System.err.println("Failed to fetch data: " + response.statusCode());
            }
        } catch (Exception e) {
            System.err.println("Error serializing message: " + e.getMessage());
        }
        return null;
    }

    public static void main(String[] args) {
        OpenMeteoProducer producer = new OpenMeteoProducer();
        producer.emit();
    }
}