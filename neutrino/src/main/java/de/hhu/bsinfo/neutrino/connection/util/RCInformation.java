package de.hhu.bsinfo.neutrino.connection.util;

import java.nio.ByteBuffer;
import java.util.StringJoiner;

public class RCInformation {

    private final byte portNumber;
    private final short localId;
    private final int queuePairNumber;

    public RCInformation(byte portNumber, short localId, int queuePairNumber) {
        this.portNumber = portNumber;
        this.localId = localId;
        this.queuePairNumber = queuePairNumber;
    }

    public RCInformation(ByteBuffer buffer) {
        portNumber = buffer.get();
        localId = buffer.getShort();
        queuePairNumber = buffer.getInt();
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

    @Override
    public String toString() {
        return new StringJoiner(", ", RCInformation.class.getSimpleName() + "[", "]")
                .add("portNumber=" + portNumber)
                .add("localId=" + localId)
                .add("queuePairNumber=" + queuePairNumber)
                .toString();
    }
}
