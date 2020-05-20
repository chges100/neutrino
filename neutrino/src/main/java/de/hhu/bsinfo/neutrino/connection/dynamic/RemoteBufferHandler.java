package de.hhu.bsinfo.neutrino.connection.dynamic;

import de.hhu.bsinfo.neutrino.buffer.BufferInformation;
import org.jctools.maps.NonBlockingHashMapLong;

/**
 * Handler for information about RDMA buffers on remote nodes.
 *
 * @author Christian Gesse
 */
public class RemoteBufferHandler {

    /**
     * Hashmap for information about remote buffers - uses local ids as keys
     */
    private final NonBlockingHashMapLong<BufferInformation> remoteBuffers = new NonBlockingHashMapLong<>();

    /**
     * Instance of dynamic connection managament
     */
    private final DynamicConnectionManager dcm;

    /**
     * Instantiates a new Remote buffer handler.
     *
     * @param dcm instance of dynamic connection management
     */
    protected RemoteBufferHandler(DynamicConnectionManager dcm) {
        this.dcm = dcm;
    }

    /**
     * Gets information about a remote buffer
     *
     * @param remoteLocalId the remote local id of the node that holds the buffer
     * @return the information about the remote buffer
     */
    protected BufferInformation getBufferInfo(short remoteLocalId) {
        return remoteBuffers.get(remoteLocalId);
    }

    /**
     * Checks if buffer information is available a remote node
     *
     * @param remoteLocalId the remote local id of ther remote node
     * @return indicator if buffer information is available
     */
    protected boolean hasBufferInfo(short remoteLocalId) {
        return remoteBuffers.containsKey(remoteLocalId);
    }

    /**
     * Register information about remote buffer
     *
     * @param remoteLocalId the remote local id of the remote node
     * @param bufferInfo    the buffer information to register
     */
    protected void registerBufferInfo(short remoteLocalId, BufferInformation bufferInfo) {
        remoteBuffers.put(remoteLocalId, bufferInfo);
    }
}
