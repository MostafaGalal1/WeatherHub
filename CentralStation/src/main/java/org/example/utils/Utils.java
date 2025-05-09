package org.example.utils;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

public class Utils {
    public static byte[] readFromFile(File file, long position, int size) throws IOException {
        try (RandomAccessFile requiredFile = new RandomAccessFile(file, "r")) {
            requiredFile.seek(position);
            byte[] value = new byte[size];
            requiredFile.readFully(value);
            requiredFile.close();
            return value;
        }
    }

    public static void writeToFile(RandomAccessFile randomAccessFile, Long key, byte[] value) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(8 + 8 + 4 + value.length);
        buffer.putLong(System.currentTimeMillis());
        buffer.putLong(key); // key
        buffer.putInt(value.length); // value
        buffer.put(value);
        buffer.flip(); // switch from writing to reading
        randomAccessFile.seek(randomAccessFile.length());
        randomAccessFile.write(buffer.array());
    }

}
