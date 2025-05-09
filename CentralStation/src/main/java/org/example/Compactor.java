package org.example;

import org.example.model.KeyDirValue;
import org.example.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import static org.example.Constants.*;

public class Compactor {

    private final ExecutorService executor;
    private final Map<Long, KeyDirValue> keyDirMap;
    private File activeFile;
    private final Logger logger = LoggerFactory.getLogger(Compactor.class);

    public Compactor(Map<Long, KeyDirValue> keyDirMap) {
        this.executor = Executors.newSingleThreadExecutor();
        this.keyDirMap = keyDirMap;
    }

    public void startCompaction(File activeFile) {
        this.activeFile = activeFile;
        executor.submit(this::compactFiles);
    }

    private void compactFiles() {
        try {
            // List all segmentFiles in the directory and filter for data segmentFiles
            File[] segmentFiles = new File(BIT_CASK_DIR).listFiles((dir, name) -> name.endsWith(BIT_CASK_EXTENSION));

            if (segmentFiles == null || segmentFiles.length == 0) {
                logger.debug("No data segmentFiles to compact.");
                return;
            }

            // If the number of data segmentFiles exceeds the threshold, start compaction
            if (segmentFiles.length > COMPACTION_THRESHOLD) {
                logger.info("Compaction needed. Merging segmentFiles...");
                mergeFiles(List.of(segmentFiles));
            }
        } catch (IOException e) {
            logger.error("Error during file merging process", e);
            throw new RuntimeException("Compaction failed", e);
        }
    }

    // Merge the old immutable files into a new compacted file
    private void mergeFiles(List<File> segmentFiles) throws IOException {
        // Create a new file for the compacted data
        File mergedFile = new File(BIT_CASK_DIR, System.currentTimeMillis() + BIT_CASK_EXTENSION);
        Map<Long, KeyDirValue> newKeyDir = new HashMap<>();
        try (RandomAccessFile outputFile = new RandomAccessFile(mergedFile, "rw")) {
            for (Map.Entry<Long, KeyDirValue> entry : keyDirMap.entrySet()) {
                KeyDirValue keyDirValue = entry.getValue();
                if (!keyDirValue.fileId().equals(activeFile)) {
                    // Read the value
                    byte[] value = Utils.readFromFile(
                            keyDirValue.fileId(), keyDirValue.valuePosition(), keyDirValue.valueSize()
                    );

                    // Write the value to the new file
                    long currentValuePos = outputFile.getFilePointer() + 8 + 8 + 4;
                    Utils.writeToFile(outputFile, entry.getKey(), value);
                    newKeyDir.put(entry.getKey(), new KeyDirValue(
                            mergedFile, currentValuePos, keyDirValue.valueSize(), keyDirValue.timeStamp()
                    ));
                }
            }

            // After merging, update the keyDir
            updateKeyDirectory(newKeyDir);

            // Remove the old files after successful compaction
            cleanUpOldFiles(getFilesToMerge(segmentFiles));

            logger.info("Compaction completed successfully. New file: {}", mergedFile.getName());
        }
    }

    // Update the key directory after merging files
    private void updateKeyDirectory(Map<Long, KeyDirValue> newKeyDir) {
        this.keyDirMap.putAll(newKeyDir);
    }

    private List<File> getFilesToMerge(List<File> segmentFiles) {
        // Merge all non-active segmentFiles
        List<File> filesToMerge = new ArrayList<>();
        for (File file : segmentFiles) {
            if (!file.equals(activeFile)) {
                filesToMerge.add(file);
            }
        }
        return filesToMerge;
    }

    // Clean up old files after compaction
    private void cleanUpOldFiles(List<File> filesToMerge) {
        for (File file : filesToMerge) {
            if (file.exists() && file.delete()) {
                logger.info("Deleted old file: {}", file.getName());
            }
        }
    }
}
