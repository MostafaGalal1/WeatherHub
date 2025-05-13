package org.example.model;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public record WeatherMessage(
        Long stationId,
        Long sNo,
        String batteryStatus,
        Long statusTimestamp,
        WeatherData weather
) {

    public byte[] toByteArray() {
        byte[] batteryStatusBytes = batteryStatus.getBytes(StandardCharsets.UTF_8);
        byte[] weatherBytes = weather.toByteArray();

        int totalSize = 8 + 8 + 4 + batteryStatusBytes.length + 8 + weatherBytes.length;
        ByteBuffer buffer = ByteBuffer.allocate(totalSize);

        buffer.putLong(stationId);
        buffer.putLong(sNo);
        buffer.putInt(batteryStatusBytes.length);
        buffer.put(batteryStatusBytes);
        buffer.putLong(statusTimestamp);
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
                "stationId=" + stationId +
                ", sNo=" + sNo +
                ", batteryStatus='" + batteryStatus + '\'' +
                ", statusTimestamp=" + statusTimestamp +
                ", weather=" + weather +
                '}';
    }
}