package org.example;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

public class WeatherArchiver {
    private static final int BATCH_SIZE = 10_000;
    private static final List<GenericRecord> buffer = new ArrayList<>();
    private static final Schema schema;

    static {
        try {
            schema = new Schema.Parser().parse(new File("src/main/resources/weather_status.avsc"));
        } catch (Exception e) {
            throw new RuntimeException("Failed to load Avro schema", e);
        }
    }

    public static void receiveStatus(Map<String, Object> statusData) throws Exception {
        GenericRecord record = new GenericData.Record(schema);
        record.put("station_id", (Long) statusData.get("station_id"));
        record.put("s_no", (Long) statusData.get("s_no"));
        record.put("battery_status", statusData.get("battery_status").toString());
        record.put("status_timestamp", (Long) statusData.get("status_timestamp"));

        // Create nested record
        Schema weatherSchema = schema.getField("weather").schema();
        GenericRecord weather = new GenericData.Record(weatherSchema);
        Map<String, Object> weatherData = (Map<String, Object>) statusData.get("weather");
        weather.put("humidity", (Integer) weatherData.get("humidity"));
        weather.put("temperature", (Integer) weatherData.get("temperature"));
        weather.put("wind_speed", (Integer) weatherData.get("wind_speed"));
        record.put("weather", weather);

        buffer.add(record);

        if (buffer.size() >= BATCH_SIZE) {
            flushToParquet();
        }
    }

    private static void flushToParquet() throws Exception {
        if (buffer.isEmpty()) return;

        // Group records by station_id
        Map<Long, List<GenericRecord>> grouped = buffer.stream()
            .collect(Collectors.groupingBy(r -> (Long) r.get("station_id")));

        String date = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
        String hour = LocalDateTime.now().format(DateTimeFormatter.ofPattern("HH"));

        for (Map.Entry<Long, List<GenericRecord>> entry : grouped.entrySet()) {
            Long stationId = entry.getKey();
            List<GenericRecord> records = entry.getValue();

            String dirPath = String.format("data/date=%s/hour=%s/station_id=%d/", date, hour, stationId);
            new File(dirPath).mkdirs();

            String uniqueFileName = "weather_" + UUID.randomUUID() + ".parquet";
            String fullPath = dirPath + uniqueFileName;
            try (ParquetWriter<GenericRecord> writer = AvroParquetWriter
                    .<GenericRecord>builder(new Path(fullPath + "weather.parquet"))
                    .withSchema(schema)
                    .build()) {

                for (GenericRecord record : records) {
                    writer.write(record);
                }
            }
        }

        buffer.clear();
    }


    // Simulate receiving weather status data (for testing purposes)
    public static void main(String[] args) throws Exception {
        for (int i = 0; i < 10000; i++) {
            Map<String, Object> status = new HashMap<>();
            status.put("station_id", (long) (i % 10 + 1));
            status.put("s_no", (long) i);
            
            String batteryStatus;
            if (i < 3000) {
                batteryStatus = "low";
            } else if (i < 7000) {
                batteryStatus = "medium";
            } else {
                batteryStatus = "high";
            }

            status.put("battery_status", batteryStatus);
            status.put("status_timestamp", System.currentTimeMillis() / 1000);

            Map<String, Object> weather = new HashMap<>();
            weather.put("humidity", 35);
            weather.put("temperature", 100);
            weather.put("wind_speed", 13);
            status.put("weather", weather);

            receiveStatus(status);
        }

        System.out.println("Finished writing 10K weather records.");
    }
}
