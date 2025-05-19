package org.example.utils;

import org.example.model.HintFileEntry;
import org.example.model.KeyDirValue;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Map;

import static org.example.Constants.*;

public class Utils {
    public static byte[] readFromFile(File file, long position, int size) throws IOException {
        try (RandomAccessFile requiredFile = new RandomAccessFile(file, "r")) {
            requiredFile.seek(position);
            byte[] value = new byte[size];
            requiredFile.readFully(value);
            return value;
        }
    }

    public static void writeToFile(RandomAccessFile randomAccessFile, Long key, byte[] value, Long messageTimeStamp) throws IOException {
        // [message_timestamp][key][value_size][value]
        ByteBuffer buffer = ByteBuffer.allocate(NUM_BYTES_VALUE_WRITE_START_AFTER + value.length);
        buffer.putLong(messageTimeStamp);
        buffer.putLong(key); // key
        buffer.putInt(value.length); // value
        buffer.put(value);
        buffer.flip(); // switch from writing to reading
        randomAccessFile.seek(randomAccessFile.length());
        randomAccessFile.write(buffer.array());
    }

    public static void readHintFileIntoMap(File file, long position, int size, Map<Long, KeyDirValue> keyDirMap) throws IOException {
        String baseFileName = removeExtension(file.getName());
        File fileId = new File(BIT_CASK_DIR, baseFileName + BIT_CASK_EXTENSION);

        try (RandomAccessFile requiredFile = new RandomAccessFile(file, "r")) {
            long fileLength = requiredFile.length();
            while (position < fileLength) {
                requiredFile.seek(position);
                byte[] byteArray = new byte[size];
                requiredFile.readFully(byteArray);
                HintFileEntry hintFileEntry = HintFileEntry.fromByteArray(ByteBuffer.wrap(byteArray));
                keyDirMap.put(hintFileEntry.stationId(), new KeyDirValue(
                        fileId, hintFileEntry.valuePosition(), hintFileEntry.valueSize(), hintFileEntry.timeStamp()
                ));

                position += size;
            }
        }
    }

    public static void writeToHintFile(RandomAccessFile randomAccessFile, Long key, byte[] value) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES + value.length);
        buffer.putLong(key); // key
        buffer.put(value);
        buffer.flip(); // switch from writing to reading
        randomAccessFile.seek(randomAccessFile.length());
        randomAccessFile.write(buffer.array());
    }

    // Helper method to extract filename without extension
    public static String removeExtension(String fileName) {
        int dotIndex = fileName.lastIndexOf('.');
        return (dotIndex > 0) ? fileName.substring(0, dotIndex) : fileName;
    }

    public static boolean createDirectory(String directoryName) {
        File dir = new File(directoryName);
        if (!dir.exists()) {
            boolean created = dir.mkdirs();
            if (!created) {
                throw new RuntimeException("Failed to create storage directory: " + directoryName);
            }
            return true;
        }
        return false;
    }
}
