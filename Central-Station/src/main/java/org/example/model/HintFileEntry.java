package org.example.model;

import java.nio.ByteBuffer;

public record HintFileEntry(
        Long stationId, // key
        Long valuePosition,
        Integer valueSize,
        Long timeStamp
) {

    public byte[] toByteArray() {
        ByteBuffer buffer = ByteBuffer.allocate(3 * Long.BYTES + Integer.BYTES);
        buffer.putLong(stationId);
        buffer.putLong(valuePosition);
        buffer.putInt(valueSize);
        buffer.putLong(timeStamp);
        return buffer.array();
    }

    public byte[] valueToByteArray() {
        ByteBuffer buffer = ByteBuffer.allocate(2 * Long.BYTES + Integer.BYTES);
        buffer.putLong(valuePosition);
        buffer.putInt(valueSize);
        buffer.putLong(timeStamp);
        return buffer.array();
    }


    public static HintFileEntry fromByteArray(ByteBuffer buffer) {
        Long stationId = buffer.getLong();
        Long valuePosition = buffer.getLong();
        Integer valueSize = buffer.getInt();
        Long timeStamp = buffer.getLong();
        return new HintFileEntry(stationId, valuePosition, valueSize, timeStamp);
    }

    public static int getSize() {
        return 3 * Long.BYTES + Integer.BYTES;
    }

    @Override
    public String toString() {
        return "HintFileEntry{" +
                "station_id=" + stationId +
                ", valuePosition=" + valuePosition +
                ", valueSize=" + valueSize +
                ", timeStamp=" + timeStamp +
                '}';
    }

}
