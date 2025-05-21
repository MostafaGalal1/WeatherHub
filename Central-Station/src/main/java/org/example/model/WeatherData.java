package org.example.model;

import java.nio.ByteBuffer;

public record WeatherData(
        byte humidity, // Between 0-100
        short temperature, // -32,768 to 32,767
        short wind_speed
) {
    public byte[] toByteArray() {
        ByteBuffer buffer = ByteBuffer.allocate(Byte.BYTES + Short.BYTES * 2);
        buffer.put(humidity);
        buffer.putShort(temperature);
        buffer.putShort(wind_speed);
        return buffer.array();
    }

    public static WeatherData fromByteArray(ByteBuffer buffer) {
        int expectedLength = Byte.BYTES + Short.BYTES * 2;
        if (buffer.array().length < expectedLength) {
            throw new IllegalArgumentException("Invalid byte array length: " + buffer.array().length);
        }

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
                ", wind_speed=" + wind_speed +
                '}';
    }

}
