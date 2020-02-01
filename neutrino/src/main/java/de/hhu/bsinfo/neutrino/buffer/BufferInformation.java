package de.hhu.bsinfo.neutrino.buffer;

import java.nio.ByteBuffer;
import java.util.StringJoiner;

public class BufferInformation {
    private final long address;
    private final long capacity;
    private final int remoteKey;

    public BufferInformation(long address, long capacity, int remoteKey) {
        this.address = address;
        this.capacity = capacity;
        this.remoteKey = remoteKey;
    }

    public BufferInformation(ByteBuffer buffer) {
        address = buffer.getLong();
        capacity = buffer.getLong();
        remoteKey = buffer.getInt();
    }

    public BufferInformation(RegisteredBuffer buffer) {
        address = buffer.getHandle();
        capacity = buffer.capacity();
        remoteKey = buffer.getRemoteKey();
    }

    public long getAddress() {
        return address;
    }

    public int getRemoteKey() {
        return remoteKey;
    }

    public long getCapacity() {
        return capacity;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", BufferInformation.class.getSimpleName() + "[", "]")
                .add("address=" + address)
                .add("capacity=" + capacity)
                .add("remoteKey=" + remoteKey)
                .toString();
    }
}
