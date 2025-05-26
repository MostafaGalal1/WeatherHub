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
    private KafkaProducer<String, String> producer;
    private long lastFetchTime = 0;
    private WeatherData cachedWeatherData = null;
    private String cachedBatteryStatus = null;
    private static final long CACHE_VALIDITY_MS = Duration.ofMinutes(15).toMillis();


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

        this.producer = new KafkaProducer<>(props);

        this.SCHEDULER.scheduleAtFixedRate(() -> {
            String message = getMessage();
            if (message != null) {
                ProducerRecord<String, String> record = new ProducerRecord<>("Weather-Metrics", message);
                this.producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        System.err.println("Failed to send to main topic: " + exception.getMessage());

                        ProducerRecord<String, String> dltRecord = new ProducerRecord<>("Weather-Metrics-DLT", message);
                        producer.send(dltRecord, (dltMetadata, dltException) -> {
                            if (dltException != null) {
                                System.err.println("Failed to send to DLT: " + dltException.getMessage());
                            } else {
                                System.out.println("Message sent to DLT: " + message);
                            }
                        });
                    } else {
                        System.out.println("Message sent to main topic: " + message);
                    }
                });
            }
        }, 0, 1, TimeUnit.SECONDS);
    }

    private String getMessage() {
        long currentTime = System.currentTimeMillis();
        if (cachedWeatherData != null && (currentTime - lastFetchTime) < CACHE_VALIDITY_MS) {
            System.out.println("Using cached weather data");

            Long messageID = this.ID_GENERATOR.incrementAndGet();

            WeatherMessage weatherMessage = new WeatherMessage(10L, messageID, cachedBatteryStatus, currentTime, cachedWeatherData);

            try {
                return objectMapper.writeValueAsString(weatherMessage);
            } catch (Exception e) {
                System.err.println("Error serializing cached message: " + e.getMessage());
                return null;
            }
        }

        try {
            String OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast?latitude=31.2001&longitude=29.9187&current=relative_humidity_2m,temperature_2m,wind_speed_10m&timezone=Africa%2FCairo&forecast_days=16";
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(OPEN_METEO_URL))
                    .timeout(Duration.ofSeconds(10))
                    .GET()
                    .build();

            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 200) {
                double p = RANDOM.nextDouble();
                cachedBatteryStatus =
                        p < 0.3 ? BatteryStatus.LOW.name() :
                                p < 0.7 ? BatteryStatus.MEDIUM.name() :
                                        BatteryStatus.HIGH.name();

                JsonNode root = objectMapper.readTree(response.body());
                JsonNode current = root.path("current");

                int humidity = current.path("relative_humidity_2m").asInt();
                int temperature = (int) Math.round(current.path("temperature_2m").asDouble());
                int windSpeed = (int) Math.round(current.path("wind_speed_10m").asDouble());

                cachedWeatherData = new WeatherData(humidity, temperature, windSpeed);
                lastFetchTime = currentTime;

                Long messageID = this.ID_GENERATOR.incrementAndGet();

                WeatherMessage weatherMessage = new WeatherMessage(10L, messageID, cachedBatteryStatus, currentTime, cachedWeatherData);

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