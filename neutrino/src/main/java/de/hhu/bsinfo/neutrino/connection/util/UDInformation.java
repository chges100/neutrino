package de.hhu.bsinfo.neutrino.connection.util;

import java.nio.ByteBuffer;
import java.util.StringJoiner;

/**
 * Object storing all important information of an UD queue pair.
 */
public class UDInformation {

    /**
     * Port number that is used
     */
    private final byte portNumber;
    /**
     * Local id if the node
     */
    private final short localId;
    /**
     * Number of the queue pair
     */
    private final int queuePairNumber;
    /**
     * Key of the queue pair
     */
    private final int queuePairKey;

    /**
     * Size of one instance of UD information
     */
    public static final int SIZE = Byte.BYTES + Short.BYTES + Integer.BYTES + Integer.BYTES;

    /**
     * Instantiates a new UD information.
     *
     * @param portNumber      the port number
     * @param localId         the local id
     * @param queuePairNumber the queue pair number
     * @param queuePairKey    the queue pair key
     */
    public UDInformation(byte portNumber, short localId, int queuePairNumber, int queuePairKey) {
        this.portNumber = portNumber;
        this.localId = localId;
        this.queuePairNumber = queuePairNumber;
        this.queuePairKey = queuePairKey;
    }

    /**
     * Wraps a new UD information from a buffer
     *
     * @param buffer the buffer containing the information
     */
    public UDInformation(ByteBuffer buffer) {
        portNumber = buffer.get();
        localId = buffer.getShort();
        queuePairNumber = buffer.getInt();
        queuePairKey = buffer.getInt();
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
     * Gets queue pair key.
     *
     * @return the queue pair key
     */
    public int getQueuePairKey() {
        return queuePairKey;
    }

    /**
     * Gets content of information as string
     *
     * @return string containing the information
     */
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
