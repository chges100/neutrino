package de.hhu.bsinfo.neutrino.connection.util;

import de.hhu.bsinfo.neutrino.connection.ReliableConnection;

import java.nio.ByteBuffer;
import java.util.StringJoiner;


/**
 * Object that holds all important information about a reliable connection.
 *
 * @author Christian Gesse
 */
public class RCInformation {

    /**
     * Port number in InfiniBand
     */
    private final byte portNumber;
    /**
     * Local id of the node
     */
    private final short localId;
    /**
     * Number of the queue pair
     */
    private final int queuePairNumber;

    /**
     * Size of RCInformation instance
     */
    public static final int SIZE = Byte.BYTES + Short.BYTES + Integer.BYTES;

    /**
     * Instantiates a new RC information.
     *
     * @param portNumber      the port number
     * @param localId         the local id
     * @param queuePairNumber the queue pair number
     */
    public RCInformation(byte portNumber, short localId, int queuePairNumber) {
        this.portNumber = portNumber;
        this.localId = localId;
        this.queuePairNumber = queuePairNumber;
    }

    /**
     * Instantiates a new RC information and gets data from existing connection
     *
     * @param connection the reliable connection
     */
    public RCInformation(ReliableConnection connection) {
        this.portNumber = (byte) 1;
        this.localId = connection.getPortAttributes().getLocalId();
        this.queuePairNumber = connection.getQueuePair().getQueuePairNumber();
    }

    /**
     * Wraps a new RC information from a given buffer
     *
     * @param buffer the buffer containing the information
     */
    public RCInformation(ByteBuffer buffer) {
        portNumber = buffer.get();
        localId = buffer.getShort();
        queuePairNumber = buffer.getInt();
    }

    /**
     * Gets port number.
     *
     * @return the port number
     */
    public byte getPortNumber() {
        return portNumber;
    }

    /**
     * Gets local id.
     *
     * @return the local id
     */
    public short getLocalId() {
        return localId;
    }

    /**
     * Gets queue pair number.
     *
     * @return the queue pair number
     */
    public int getQueuePairNumber() {
        return queuePairNumber;
    }

    /**
     * Gets content of information as string
     *
     * @return string containing the information
     */
    @Override
    public String toString() {
        return new StringJoiner(", ", RCInformation.class.getSimpleName() + "[", "]")
                .add("portNumber=" + portNumber)
                .add("localId=" + localId)
                .add("queuePairNumber=" + queuePairNumber)
                .toString();
    }
}
