package de.hhu.bsinfo.neutrino.connection;

import java.nio.ByteBuffer;
import java.util.StringJoiner;

public class ConnectionInformation {

    private final byte portNumber;
    private final short localId;
    private final int queuePairNumber;

    public ConnectionInformation(byte portNumber, short localId, int queuePairNumber) {
        this.portNumber = portNumber;
        this.localId = localId;
        this.queuePairNumber = queuePairNumber;
    }

    public ConnectionInformation(ByteBuffer buffer) {
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
        return new StringJoiner(", ", ConnectionInformation.class.getSimpleName() + "[", "]")
                .add("portNumber=" + portNumber)
                .add("localId=" + localId)
                .add("queuePairNumber=" + queuePairNumber)
                .toString();
    }
}
