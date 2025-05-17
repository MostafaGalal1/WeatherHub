package org.example;

import org.example.model.WeatherData;
import org.example.model.WeatherMessage;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

@SpringBootApplication
public class CentralStation {

    public static void main(String[] args) {
        ConfigurableApplicationContext context = SpringApplication.run(CentralStation.class, args);

        BitCask bitCask = context.getBean(BitCask.class);

        WeatherMessage value = new WeatherMessage(
                1L,
                1L,
                "low",
                System.currentTimeMillis(),
                new WeatherData(
                        (byte) 35,
                        (short) 100,
                        (short) 13
                )
        );

        System.out.println(bitCask.get(1L));
        System.out.println(bitCask.get(2L)); // null
    }

}
