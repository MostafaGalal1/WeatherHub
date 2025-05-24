package org.example;

import jdk.jfr.Recording;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Paths;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class FlightRecorderService {
    private Recording recording;
    private final Logger logger = LoggerFactory.getLogger(FlightRecorderService.class);

    public void startRecording() {
        recording = new Recording();
        recording.setName("KafkaPollingRecording");

        // --- Memory Allocation (for top memory-using classes)
        recording.enable("jdk.ObjectAllocationInNewTLAB").withStackTrace();
        recording.enable("jdk.ObjectAllocationOutsideTLAB").withStackTrace();
        // --- GC Information
        recording.enable("jdk.GarbageCollection");
        // --- I/O operations
        recording.enable("jdk.SocketRead").withThreshold(Duration.ofMillis(0));
        recording.enable("jdk.SocketWrite").withThreshold(Duration.ofMillis(0));
        recording.enable("jdk.FileRead").withThreshold(Duration.ofMillis(0));
        recording.enable("jdk.FileWrite").withThreshold(Duration.ofMillis(0));

        recording.enable("jdk.ExecutionSample").withPeriod(Duration.ofMillis(10));
        recording.setDuration(Duration.ofMinutes(1));

        recording.start();
        logger.debug("JFR Recording has started!");

        Executors.newSingleThreadScheduledExecutor().schedule(() -> {
            try {
                recording.dump(Paths.get("central-station.jfr"));
                logger.debug("Dumping JFR file is done");
            } catch (Exception e) {
                logger.error("Error: ", e);
            }
        }, 1, TimeUnit.MINUTES);
    }
}
