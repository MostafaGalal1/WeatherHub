package org.example;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class CentralStation {
    public static void main(String[] args) {
        new FlightRecorderService().startRecording();
        SpringApplication.run(CentralStation.class, args);
    }
}
