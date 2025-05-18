package org.example;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.example.model.WeatherData;
import org.example.model.WeatherMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

import static org.example.Constants.BATCH_SIZE;

public class WeatherArchiver {
    private final List<GenericRecord> buffer = new ArrayList<>();
    private final Schema schema;
    private final Logger logger = LoggerFactory.getLogger(WeatherArchiver.class);

    public WeatherArchiver() {
        try {
            this.schema = new Schema.Parser().parse(
                    // Stable and general way to retrieve resource files
                    WeatherArchiver.class.getClassLoader().getResourceAsStream("weather_status.avsc")
            );
        } catch (Exception e) {
            throw new RuntimeException("Failed to load Avro schema", e);
        }
    }

    public void receiveStatus(WeatherMessage weatherMessage) {
        GenericRecord record = new GenericData.Record(this.schema);
        record.put("station_id", weatherMessage.station_id());
        record.put("s_no", weatherMessage.s_no());
        record.put("battery_status", weatherMessage.battery_status());
        record.put("status_timestamp", weatherMessage.status_timestamp());

        // Create nested record
        Schema weatherSchema = this.schema.getField("weather").schema();
        GenericRecord weather = new GenericData.Record(weatherSchema);
        weather.put("humidity", weatherMessage.weather().humidity());
        weather.put("temperature", weatherMessage.weather().temperature());
        weather.put("wind_speed", weatherMessage.weather().wind_speed());
        record.put("weather", weather);

        this.buffer.add(record);

        if (this.buffer.size() >= BATCH_SIZE) {
            flushToParquet();
        }
    }

    private void flushToParquet() {
        if (this.buffer.isEmpty()) return;

        // Group records by station_id
        Map<Long, List<GenericRecord>> grouped = this.buffer.stream()
                .collect(Collectors.groupingBy(r -> (Long) r.get("station_id")));

        String date = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
        String hour = LocalDateTime.now().format(DateTimeFormatter.ofPattern("HH"));

        for (Map.Entry<Long, List<GenericRecord>> entry : grouped.entrySet()) {
            Long stationId = entry.getKey();
            List<GenericRecord> records = entry.getValue();

            String dirPath = String.format("data/date=%s/hour=%s/station_id=%d/", date, hour, stationId);
            File directory = new File(dirPath);
            if (!directory.exists() && !directory.mkdirs()) {
                logger.warn("Failed to create directory: {}", dirPath);
                continue;
            }

            String uniqueFileName = "weather_" + UUID.randomUUID() + ".parquet";
            String fullPath = dirPath + uniqueFileName;

            Path filePath = new Path(fullPath);
            Configuration conf = new Configuration();
            try (
                    ParquetWriter<GenericRecord> writer = AvroParquetWriter
                            .<GenericRecord>builder(HadoopOutputFile.fromPath(filePath, conf))
                            .withConf(conf)
                            .withSchema(this.schema)
                            .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                            .withCompressionCodec(CompressionCodecName.GZIP)
                            .build()
            ) {
                for (GenericRecord record : records) {
                    writer.write(record);
                }
                logger.info("Wrote {} records to {}", records.size(), fullPath);
            } catch (IOException e) {
                logger.error("Failed to write Parquet file: {}", fullPath, e);
            }
        }

        this.buffer.clear();
    }

    // Simulate receiving weather status data (for testing purposes)
    public static void main(String[] args) {
        WeatherArchiver weatherArchiver = new WeatherArchiver();
        WeatherMessage weatherMessage;
        for (int i = 0; i < 10000; i++) {
            weatherMessage = new WeatherMessage(
                    (long) (i % 10 + 1),
                    (long) i,
                    (i < 3000) ? "low" : (i < 7000) ? "medium" : "high",
                    System.currentTimeMillis() / 1000,
                    new WeatherData(
                            (byte) 35, (short) 100, (short) 13
                    )
            );
            weatherArchiver.receiveStatus(weatherMessage);
        }
        System.out.println("Finished writing 10K weather records.");
    }
}
