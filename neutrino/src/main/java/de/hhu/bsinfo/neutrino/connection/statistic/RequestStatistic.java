package de.hhu.bsinfo.neutrino.connection.statistic;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Statistic correspomding to all work request on one remote node
 *
 * @author Christian Gesse
 */
public class RequestStatistic {

    /**
     * local id of the remote node corresponding to the work request
     */
    public final short remoteLocalId;

    /**
     * bytes written via rdma on this remote node
     */
    public final AtomicLong rdmaBytesWritten = new AtomicLong(0);
    /**
     * bytes read via rdma from this remote node
     */
    public final AtomicLong rdmaBytesRead = new AtomicLong(0);
    /**
     * bytes sent to this remote node
     */
    public final AtomicLong bytesSent = new AtomicLong(0);
    /**
     * bytes received from this remote node
     */
    public final AtomicLong bytesReceived = new AtomicLong(0);

    /**
     * Count of all rdma write operations on this remote node
     */
    public final AtomicLong rdmaWriteCount = new AtomicLong(0);
    /**
     * Count of all rdma read operations on this remote node
     */
    public final AtomicLong rdmaReadCount = new AtomicLong(0);
    /**
     * Count of all send operations to this remote node
     */
    public final AtomicLong sendCount = new AtomicLong(0);
    /**
     * Count of all receive operations from this remote node
     */
    public final AtomicLong receiveCount = new AtomicLong(0);
    /**
     * Count of all other operation on this remote node
     */
    public final AtomicLong otherOpCount = new AtomicLong(0);
    /**
     * Count of all work requests finished with error state corresponding with this remote node
     */
    public final AtomicLong errorCount = new AtomicLong(0);

    /**
     * Constructor for the request statistic
     * @param remoteLocalId the local id of the remote node
     */
    public RequestStatistic(short remoteLocalId) {
        this.remoteLocalId = remoteLocalId;
    }
}
