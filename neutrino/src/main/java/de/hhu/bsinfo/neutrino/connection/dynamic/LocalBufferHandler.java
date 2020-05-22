package de.hhu.bsinfo.neutrino.connection.dynamic;

import de.hhu.bsinfo.neutrino.buffer.BufferInformation;
import de.hhu.bsinfo.neutrino.buffer.RegisteredBuffer;
import org.jctools.maps.NonBlockingHashMapLong;


/**
 * Handler for local buffers which are used as target for RDMA operations from remote nodes.
 * For each remote node one RDMA buffer for operations if allocated by default.
 *
 * @author Christian Gesse
 */
public class LocalBufferHandler {

    /**
     * The buffer map with remote local id as key
     */
    private final NonBlockingHashMapLong<RegisteredBuffer> localBuffers = new NonBlockingHashMapLong<>();

    /**
     * Instance of the dynamic connection management
     */
    private final DynamicConnectionManager dcm;

    /**
     * Instantiates a new Local buffer handler.
     *
     * @param dcm instance of the dynamic connection management
     */
    protected LocalBufferHandler(DynamicConnectionManager dcm) {
        this.dcm = dcm;
    }

    /**
     * Gets buffer correpsonding to a remote node
     *
     * @param remoteLocalId the remote local id
     * @return the buffer corresponding to this node
     */
    protected RegisteredBuffer getBuffer(short remoteLocalId) {
        return localBuffers.get(remoteLocalId);
    }

    /**
     * Indicates if a buffer is registered for the remote
     *
     * @param remoteLocalId the remote local id
     * @return the boolean
     */
    protected boolean hasBuffer(short remoteLocalId) {
        return localBuffers.containsKey(remoteLocalId);
    }

    /**
     * Creates and registers a buffer for the remote.
     *
     * @param remoteLocalId the remote local id
     */
    protected void createAndRegisterBuffer(short remoteLocalId) {

        // allocate buffer as memory region for RDMA access
        var buffer = dcm.allocRegisteredBuffer(0, dcm.rdmaBufferSize);
        buffer.clear();

        // register buffer into map
        var ret = localBuffers.putIfAbsent(remoteLocalId, buffer);

        // propagate buffer information to remote if necessary
        if (ret == null) {
            var bufferInfo = new BufferInformation(buffer);
            dcm.dch.sendBufferInfo(bufferInfo, dcm.getLocalId(), (short) remoteLocalId);
        } else {
            buffer.close();
        }
    }
}
