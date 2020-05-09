package de.hhu.bsinfo.neutrino.connection.dynamic;

import de.hhu.bsinfo.neutrino.buffer.BufferInformation;
import org.jctools.maps.NonBlockingHashMapLong;

public class RemoteBufferHandler {

    private final NonBlockingHashMapLong<BufferInformation> remoteBuffers = new NonBlockingHashMapLong<>();

    private final DynamicConnectionManager dcm;

    protected RemoteBufferHandler(DynamicConnectionManager dcm) {
        this.dcm = dcm;
    }

    protected BufferInformation getBufferInfo(short remoteLocalId) {
        return remoteBuffers.get(remoteLocalId);
    }

    protected boolean hasBufferInfo(short remoteLocalId) {
        return remoteBuffers.containsKey(remoteLocalId);
    }

    protected void registerBufferInfo(short remoteLocalId, BufferInformation bufferInfo) {
        remoteBuffers.put(remoteLocalId, bufferInfo);
    }
}
