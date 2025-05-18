package org.example.utils;

import org.example.model.WeatherMessage;

import java.util.List;

public class MessageValidator {

    private static final List<String> VALID_BATTERY_STATUSES = List.of("low", "medium", "high");

    public static boolean isValid(WeatherMessage msg, Long lastAddedTimestamp) {
        return isStationIdValid(msg.station_id())
                && isSNoValid(msg.s_no())
                && isBatteryStatusValid(msg.battery_status())
                && isTimestampValid(msg.status_timestamp(), lastAddedTimestamp)
                && isHumidityValid(msg.weather().humidity());
    }

    private static boolean isStationIdValid(Long id) {
        return id != null && id >= 0;
    }

    private static boolean isSNoValid(Long sNo) {
        return sNo != null && sNo >= 0;
    }

    private static boolean isBatteryStatusValid(String status) {
        return status != null && VALID_BATTERY_STATUSES.contains(status.toLowerCase());
    }

    private static boolean isTimestampValid(Long timestamp, Long lastAddedTimestamp) {
        if (timestamp == null) return false;
        return timestamp >= lastAddedTimestamp; // not old message
    }

    private static boolean isHumidityValid(byte humidity) {
        return humidity >= 0 && humidity <= 100;
    }
}
