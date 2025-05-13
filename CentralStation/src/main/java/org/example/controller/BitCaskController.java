package org.example.controller;

import org.example.BitCask;
import org.example.model.KeyDirValue;
import org.example.model.WeatherMessage;
import org.example.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.example.Constants.CLIENT_DIR;

@RestController
@RequestMapping("/bitcask-kv")
public class BitCaskController {

    private final BitCask bitCask;
    private final Logger logger = LoggerFactory.getLogger(BitCaskController.class);

    @Autowired
    public BitCaskController(BitCask bitCask) {
        this.bitCask = bitCask;
    }

    // Endpoint to view all keys and values
    @GetMapping("/view-all")
    public ResponseEntity<String> viewAll() {
        Utils.createDirectory(CLIENT_DIR);
        String fileName = System.currentTimeMillis() + ".csv";
        String filePath = Paths.get(CLIENT_DIR, fileName).toString();

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath))) {
            writer.write("Key,Value\n");
            Map<Long, KeyDirValue> keyDirMap = bitCask.getKeyDirMap();
            for (Map.Entry<Long, KeyDirValue> entry : keyDirMap.entrySet()) {
                WeatherMessage weatherMessage = bitCask.get(entry.getKey());
                writer.write(entry.getKey() + "," + weatherMessage + "\n");
            }
        } catch (IOException e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Error writing CSV file " + fileName + "\n");
        }

        return ResponseEntity.ok("CSV file saved at " + filePath + "\n");
    }

    // Endpoint to view the value of a specific key
    @GetMapping("/view")
    public ResponseEntity<String> viewKey(@RequestParam("key") Long key) {
        Utils.createDirectory(CLIENT_DIR);
        WeatherMessage weatherMessage = bitCask.get(key);

        if (weatherMessage == null) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body("Key not found\n");
        }

        return ResponseEntity.ok(weatherMessage.toString());
    }

    // Endpoint to simulate multiple clients querying the data concurrently
    @GetMapping("/perf")
    public ResponseEntity<String> performanceTest(@RequestParam("clients") int clients) {
        Utils.createDirectory(CLIENT_DIR);
        ExecutorService executorService = Executors.newFixedThreadPool(clients);
        List<Future<String>> futures = new ArrayList<>();

        for (int i = 0; i < clients; i++) {
            final int clientId = i;
            futures.add(executorService.submit(() -> {
                String fileName = System.currentTimeMillis() + "_thread_" + clientId + ".csv";
                String filePath = Paths.get(CLIENT_DIR, fileName).toString();

                try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath))) {
                    writer.write("Key,Value\n");
                    Map<Long, KeyDirValue> keyDirMap = bitCask.getKeyDirMap();
                    for (Map.Entry<Long, KeyDirValue> entry : keyDirMap.entrySet()) {
                        WeatherMessage weatherMessage = bitCask.get(entry.getKey());
                        writer.write(entry.getKey() + "," + weatherMessage + "\n");
                    }
                } catch (IOException e) {
                    return "Error writing CSV file for client " + clientId + "\n";
                }
                return "Data saved for client " + clientId + "\n";
            }));
        }

        executorService.shutdown();

        try {
            for (Future<String> future : futures) {
                logger.info(future.get());
            }
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Error during performance test\n");
        }

        return ResponseEntity.ok("Performance test completed\n");
    }
}
