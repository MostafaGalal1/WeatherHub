package org.example.model;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public record WeatherMessage(
        Long station_id,
        Long s_no,
        String battery_status,
        Long status_timestamp,
        WeatherData weather
) {

    public byte[] toByteArray() {
        byte[] batteryStatusBytes = battery_status.getBytes(StandardCharsets.UTF_8);
        byte[] weatherBytes = weather.toByteArray();

        int totalSize = 8 + 8 + 4 + batteryStatusBytes.length + 8 + weatherBytes.length;
        ByteBuffer buffer = ByteBuffer.allocate(totalSize);

        buffer.putLong(station_id);
        buffer.putLong(s_no);
        buffer.putInt(batteryStatusBytes.length);
        buffer.put(batteryStatusBytes);
        buffer.putLong(status_timestamp);
        buffer.put(weatherBytes);

        return buffer.array();
    }

    public static WeatherMessage fromByteArray(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);

        long stationId = buffer.getLong();
        long sNo = buffer.getLong();

        // Battery Status length then actual data
        int batteryStatusLength = buffer.getInt();
        byte[] batteryStatusBytes = new byte[batteryStatusLength];
        buffer.get(batteryStatusBytes);
        String batteryStatus = new String(batteryStatusBytes, StandardCharsets.UTF_8);

        long statusTimestamp = buffer.getLong();

        // Now WeatherData
        WeatherData weather = WeatherData.fromByteArray(buffer);

        return new WeatherMessage(stationId, sNo, batteryStatus, statusTimestamp, weather);
    }

    @Override
    public String toString() {
        return "WeatherMessage{" +
                "station_id=" + station_id +
                ", s_no=" + s_no +
                ", battery_status='" + battery_status + '\'' +
                ", status_timestamp=" + status_timestamp +
                ", weather=" + weather +
                '}';
    }
}