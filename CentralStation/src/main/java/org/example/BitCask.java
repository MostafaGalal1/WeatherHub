package org.example;

import org.example.model.WeatherMessage;

public interface BitCask {
    WeatherMessage get(Long key);
    void put(WeatherMessage weatherMessage);
}
