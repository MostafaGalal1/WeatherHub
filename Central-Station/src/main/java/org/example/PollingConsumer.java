package org.example;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.example.model.KeyDirValue;
import org.example.model.WeatherMessage;
import org.example.utils.MessageValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.Executors;

@Service
public class PollingConsumer {
    private KafkaConsumer<String, String> kafkaConsumer;
    private KafkaProducer<String, String> kafkaProducer;
    private final BitCask bitCask;
    private final ObjectMapper objectMapper;
    private final WeatherArchiver weatherArchiver;
    private final Logger logger = LoggerFactory.getLogger(PollingConsumer.class);

    public PollingConsumer() {
        initConsumer();
        initProducerForInvalidMessages();
        this.bitCask = new BitCaskImp();
        this.objectMapper = new ObjectMapper();
        this.weatherArchiver = new WeatherArchiver();
    }

    private void initConsumer() {
        final String topicName = "Weather-Metrics";
        final String groupId = "bitcask-group";
        Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        this.kafkaConsumer = new KafkaConsumer<>(props);
        this.kafkaConsumer.subscribe(Collections.singletonList(topicName));
    }

    private void initProducerForInvalidMessages() {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");

        this.kafkaProducer = new KafkaProducer<>(props);
    }

    @PostConstruct
    public void start() {
        Executors.newSingleThreadExecutor().submit(() -> {
            try {
                while (true) {
                    // As each weather station should output a status message every 1 second
                    ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofSeconds(1));
                    records.forEach(record -> processMessage(record.value()));
                }
            } catch (Exception e) {
                logger.error("Error when polling a message", e);
            }
        });
    }

    private void processMessage(String message) {
        final String invalidMessagesTopic = "Invalid-Message";
        try {
            WeatherMessage weatherMessage = this.objectMapper.readValue(message, WeatherMessage.class);
            // Check if message is valid
            //  1. Discard delayed messages
            //  2. BatteryStatus should be a String of (low, medium, high)
            //  3. StationId and SNo should be Long
            //  4. Humidity between 0:100
            KeyDirValue keyDirValue = this.bitCask.getKeyDirMap().get(weatherMessage.station_id());
            Long lastAddedTimestamp = keyDirValue == null ? weatherMessage.status_timestamp() : keyDirValue.timeStamp();
            if (!MessageValidator.isValid(weatherMessage, lastAddedTimestamp)) {
                ProducerRecord<String, String> record = new ProducerRecord<>(invalidMessagesTopic, message);
                this.kafkaProducer.send(record);
                logger.debug("Invalid WeatherMessage received: {}", weatherMessage);
                return;
            }
            logger.debug("Polled WeatherMessage: {}", weatherMessage);
            this.bitCask.put(weatherMessage);
            this.weatherArchiver.receiveStatus(weatherMessage);
        } catch (JsonProcessingException e) {
            logger.error("Error when process a message: {}", e.getMessage());
        }
    }
}
