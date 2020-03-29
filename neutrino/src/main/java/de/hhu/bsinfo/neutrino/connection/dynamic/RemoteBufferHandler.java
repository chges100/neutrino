package de.hhu.bsinfo.neutrino.connection.dynamic;

import de.hhu.bsinfo.neutrino.buffer.BufferInformation;
import org.agrona.collections.Int2ObjectHashMap;

public class RemoteBufferHandler {
    private final Int2ObjectHashMap<BufferInformation> remoteBuffers;
    private final DynamicConnectionManager dcm;

    protected RemoteBufferHandler(DynamicConnectionManager dcm) {
        this.dcm = dcm;

        remoteBuffers = new Int2ObjectHashMap<>();
    }

    protected BufferInformation getBufferInfo(int remoteLocalId) {
        return remoteBuffers.get(remoteLocalId);
    }

    protected boolean hasBufferInfo(int remoteLocalId) {
        return remoteBuffers.containsKey(remoteLocalId);
    }

    protected void registerBufferInfo(int remoteLocalId, BufferInformation bufferInfo) {
        remoteBuffers.put(remoteLocalId, bufferInfo);
    }
}
