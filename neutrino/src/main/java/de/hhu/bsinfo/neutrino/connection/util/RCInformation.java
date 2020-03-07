package de.hhu.bsinfo.neutrino.connection.util;

import de.hhu.bsinfo.neutrino.connection.ReliableConnection;

import java.nio.ByteBuffer;
import java.util.StringJoiner;

public class RCInformation {

    private final byte portNumber;
    private final short localId;
    private final int queuePairNumber;

    public static final int SIZE = Byte.BYTES + Short.BYTES + Integer.BYTES;

    public RCInformation(byte portNumber, short localId, int queuePairNumber) {
        this.portNumber = portNumber;
        this.localId = localId;
        this.queuePairNumber = queuePairNumber;
    }

    public RCInformation(ReliableConnection connection) {
        this.portNumber = (byte) 1;
        this.localId = connection.getPortAttributes().getLocalId();
        this.queuePairNumber = connection.getQueuePair().getQueuePairNumber();
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
