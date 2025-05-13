package org.example.model;

public record DeadLetterMessage(String cause, Long timestamp, WeatherMessage weatherMessage) {}
