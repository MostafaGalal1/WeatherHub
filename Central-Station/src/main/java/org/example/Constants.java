package org.example;

public final class Constants {
    public static final String BIT_CASK_DIR = "data/segments";
    public static final String BIT_CASK_EXTENSION = ".bitcask";
    public static final String HINT_FILES_DIR = "data/hints";
    public static final String CLIENT_DIR = "client";
    public static final String HINT_FILE_EXTENSION = ".hint";
    public static final Integer FILE_THRESHOLD = 100 * 1024; // 10kb
    public static final Integer COMPACTION_THRESHOLD = 10;
    public static final int BATCH_SIZE = 10_000;
    public static final int NUM_BYTES_VALUE_WRITE_START_AFTER = Long.BYTES * 2 + Integer.BYTES;
}
