package org.example;

import org.example.model.HintFileEntry;
import org.example.model.KeyDirValue;
import org.example.model.WeatherMessage;
import org.example.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.example.Constants.*;

@Component
public class BitCaskImp implements BitCask {

    private static final Logger logger = LoggerFactory.getLogger(BitCaskImp.class);

    private RandomAccessFile activeFile;
    private RandomAccessFile hintFile;
    private File currentFileId; // To get the id of the segment file where the actual log entry is stored
    private final Map<Long, KeyDirValue> keyDirMap;

    public BitCaskImp() {
        keyDirMap = new ConcurrentHashMap<>();
        // If any directory is missing, we need to initialize again
        boolean isBitCaskDirExists = !Utils.createDirectory(BIT_CASK_DIR) ;
        boolean isHintDirExists = !Utils.createDirectory(HINT_FILES_DIR);
        if (isBitCaskDirExists && isHintDirExists) {
            recover();
        } else {
            initialize();
        }
        Compactor compactor = new Compactor(keyDirMap);
        compactor.startCompaction(currentFileId);
    }

    private void initialize() {
        createNewFile(String.valueOf(System.currentTimeMillis()));
    }

    @Override
    public WeatherMessage get(Long key) {
        KeyDirValue keyDirValue = keyDirMap.get(key);
        if (keyDirValue == null) return null;
        logger.info("Value = {}", keyDirValue);
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
            // Write to data file
            Utils.writeToFile(activeFile, weatherMessage.stationId(), weatherMessageBytes);
            // 3. Check addition to keyDir
            KeyDirValue keyDirValue = keyDirMap.get(weatherMessage.stationId());
            if (keyDirValue != null && keyDirValue.timeStamp() >= weatherMessage.statusTimestamp())
                return; // Correct ordering guarantee

            // Write to hint file first before updating the key directory
            HintFileEntry hintFileEntry = new HintFileEntry(
                    weatherMessage.stationId(), currentValuePos, weatherMessageBytes.length, weatherMessage.statusTimestamp());
            Utils.writeToHintFile(hintFile, hintFileEntry.stationId(), hintFileEntry.valueToByteArray());

            keyDirMap.put(weatherMessage.stationId(), new KeyDirValue(
                    currentFileId, currentValuePos, weatherMessageBytes.length, weatherMessage.statusTimestamp()
            ));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // Use hint files to recover the in-memory key directory in case of failures
    private void recover() {
        File directory = new File(HINT_FILES_DIR);

        if(!directory.exists() || !directory.isDirectory())
            return;

        File[] files = directory.listFiles();
        if (files == null)
            return;

        // Sort files by name (= timestamp)
        Arrays.sort(files);

        long position = 0;
        int size = HintFileEntry.getSize();
        for (File file : files) {
            if (file.isFile()) {
                try {
                    Utils.readHintFileIntoMap(file, position, size, this.keyDirMap);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        String baseFileName = Utils.removeExtension(files[files.length - 1].getName());
        createNewFile(baseFileName);
    }

    @Override
    public Map<Long, KeyDirValue> getKeyDirMap() {
        return keyDirMap;
    }

    private void checkFileSizeAndCreateNewIfNeeded() throws IOException {
        if (activeFile.length() > FILE_THRESHOLD) {
            activeFile.close();
            hintFile.close();
            createNewFile(String.valueOf(System.currentTimeMillis()));
        }
    }

    private void createNewFile(String baseFileName) {
        this.currentFileId = new File(BIT_CASK_DIR, baseFileName + BIT_CASK_EXTENSION);
        File hintFileId = new File(HINT_FILES_DIR, baseFileName + HINT_FILE_EXTENSION);
        try {
            this.activeFile = new RandomAccessFile(this.currentFileId, "rw");
            this.hintFile = new RandomAccessFile(hintFileId, "rw");
            logger.info("Created new file {}", this.currentFileId.getName());
        } catch (FileNotFoundException e) {
            System.err.println("File not found: " + e.getMessage());
        }
    }
}
