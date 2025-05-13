package org.example.model;

import java.nio.ByteBuffer;

public record WeatherData(
        byte humidity, // Between 0-100
        short temperature, // -32,768 to 32,767
        short windSpeed
) {
    public byte[] toByteArray() {
        ByteBuffer buffer = ByteBuffer.allocate(1 + 2 + 2);
        buffer.put(humidity);
        buffer.putShort(temperature);
        buffer.putShort(windSpeed);
        return buffer.array();
    }

    public static WeatherData fromByteArray(ByteBuffer buffer) {
        byte humidity = buffer.get();
        short temperature = buffer.getShort();
        short windSpeed = buffer.getShort();
        return new WeatherData(humidity, temperature, windSpeed);
    }

    @Override
    public String toString() {
        return "WeatherData{" +
                "humidity=" + humidity +
                ", temperature=" + temperature +
                ", windSpeed=" + windSpeed +
                '}';
    }

}
