package de.hhu.bsinfo.neutrino.connection.util;

import java.nio.ByteBuffer;
import java.util.StringJoiner;

public class UDRemoteInformation {

    private final byte portNumber;
    private final short localId;
    private final int queuePairNumber;
    private final int queuePairKey;

    public UDRemoteInformation(byte portNumber, short localId, int queuePairNumber, int queuePairKey) {
        this.portNumber = portNumber;
        this.localId = localId;
        this.queuePairNumber = queuePairNumber;
        this.queuePairKey = queuePairKey;
    }

    public UDRemoteInformation(ByteBuffer buffer) {
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
        return new StringJoiner(", ", ConnectionInformation.class.getSimpleName() + "[", "]")
                .add("portNumber=" + portNumber)
                .add("localId=" + localId)
                .add("queuePairNumber=" + queuePairNumber)
                .add("queuePairKey=" + queuePairKey)
                .toString();
    }

}
