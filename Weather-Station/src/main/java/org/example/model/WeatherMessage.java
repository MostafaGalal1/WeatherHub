package org.example.model;

public record WeatherMessage(Long station_id, Long s_no, String battery_status, Long status_timestamp, WeatherData weather) {}