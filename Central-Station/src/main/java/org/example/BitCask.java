package org.example;

import org.example.model.KeyDirValue;
import org.example.model.WeatherMessage;

import java.util.Map;

public interface BitCask {
    WeatherMessage get(Long key);
    void put(WeatherMessage weatherMessage);
    Map<Long, KeyDirValue> getKeyDirMap();
}
