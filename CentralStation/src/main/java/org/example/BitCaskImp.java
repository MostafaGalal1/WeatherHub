package org.example;

import org.example.model.KeyDirValue;
import org.example.model.WeatherMessage;
import org.example.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.example.Constants.*;

public class BitCaskImp implements BitCask {

    private static final Logger logger = LoggerFactory.getLogger(BitCaskImp.class);

    private RandomAccessFile activeFile;
    private File currentFileId; // To get the id of the segment file where the actual log entry is stored
    private final Map<Long, KeyDirValue> keyDirMap;
    private final Compactor compactor;

    public BitCaskImp() {
        this.keyDirMap = new ConcurrentHashMap<>();
        this.compactor = new Compactor(keyDirMap);
        initialization();
    }

    private void initialization() {
        File dir = new File(BIT_CASK_DIR);
        if (!dir.exists()) {
            boolean created = dir.mkdirs();
            if (!created) {
                throw new RuntimeException("Failed to create storage directory: " + BIT_CASK_DIR);
            }
        }
        // Should be called after initializing compactor
        createNewFile();
    }

    @Override
    public WeatherMessage get(Long key) {
        KeyDirValue keyDirValue = keyDirMap.get(key);
        if (keyDirValue == null) return null;

        try {
            byte[] value = Utils.readFromFile(
                    keyDirValue.fileId(), keyDirValue.valuePosition(), keyDirValue.valueSize()
            );
            return WeatherMessage.fromByteArray(value);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void put(WeatherMessage weatherMessage) {
        // String key = "station_" + weatherMessage.stationId();
        byte[] weatherMessageBytes = weatherMessage.toByteArray();

        try {
            // 1. Check file size
            checkFileSizeAndCreateNewIfNeeded();
            // 2. Append-only at the end of the file
            long currentValuePos = activeFile.getFilePointer() + 8 + 8 + 4; // Get pointer before writing
            Utils.writeToFile(activeFile, weatherMessage.stationId(), weatherMessageBytes);
            // 3. Check addition to keyDir
            KeyDirValue keyDirValue = keyDirMap.get(weatherMessage.stationId());
            if (keyDirValue != null && keyDirValue.timeStamp() >= weatherMessage.statusTimestamp())
                return; // Correct ordering guarantee
            keyDirMap.put(weatherMessage.stationId(), new KeyDirValue(
                    currentFileId, currentValuePos, weatherMessageBytes.length, weatherMessage.statusTimestamp()
            ));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void checkFileSizeAndCreateNewIfNeeded() throws IOException {
        if (activeFile.length() > FILE_THRESHOLD) {
            activeFile.close();
            createNewFile();
        }
    }

    private void createNewFile() {
        this.currentFileId = new File(BIT_CASK_DIR, System.currentTimeMillis() + BIT_CASK_EXTENSION);
        try {
            this.activeFile = new RandomAccessFile(this.currentFileId, "rw");
            logger.info("Created new file {}", this.currentFileId.getName());
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
        this.compactor.startCompaction(currentFileId);
    }
}
