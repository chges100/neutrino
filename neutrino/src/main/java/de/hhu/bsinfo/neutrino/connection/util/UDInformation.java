package de.hhu.bsinfo.neutrino.connection.util;

import java.nio.ByteBuffer;
import java.util.StringJoiner;

public class UDInformation {

    private final byte portNumber;
    private final short localId;
    private final int queuePairNumber;
    private final int queuePairKey;

    public static final int SIZE = Byte.BYTES + Short.BYTES + Integer.BYTES + Integer.BYTES;

    public UDInformation(byte portNumber, short localId, int queuePairNumber, int queuePairKey) {
        this.portNumber = portNumber;
        this.localId = localId;
        this.queuePairNumber = queuePairNumber;
        this.queuePairKey = queuePairKey;
    }

    public UDInformation(ByteBuffer buffer) {
        portNumber = buffer.get();
        localId = buffer.getShort();
        queuePairNumber = buffer.getInt();
        queuePairKey = buffer.getInt();
    }

    public byte getPortNumber() {
        return portNumber;
    }

    public short getLocalId() {
        return localId;
    }

    public int getQueuePairNumber() {
        return queuePairNumber;
    }

    public int getQueuePairKey() {
        return queuePairKey;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", UDInformation.class.getSimpleName() + "[", "]")
                .add("portNumber=" + portNumber)
                .add("localId=" + localId)
                .add("queuePairNumber=" + queuePairNumber)
                .add("queuePairKey=" + queuePairKey)
                .toString();
    }

}
