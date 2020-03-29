package de.hhu.bsinfo.neutrino.connection.dynamic;

import de.hhu.bsinfo.neutrino.buffer.BufferInformation;
import de.hhu.bsinfo.neutrino.buffer.RegisteredBuffer;
import org.agrona.collections.Int2ObjectHashMap;

public class LocalBufferHandler {
    private final Int2ObjectHashMap<RegisteredBuffer> localBuffers;
    private final DynamicConnectionManager dcm;

    protected LocalBufferHandler(DynamicConnectionManager dcm) {
        this.dcm = dcm;

        localBuffers = new Int2ObjectHashMap<>();
    }

    protected RegisteredBuffer getBuffer(int remoteLocalId) {
        return localBuffers.get(remoteLocalId);
    }

    protected boolean hasBuffer(int remoteLocalId) {
        return localBuffers.containsKey(remoteLocalId);
    }

    protected void registerBuffer(int remoteLocalId, RegisteredBuffer buffer) {
        var oldBuffer = localBuffers.put(remoteLocalId, buffer);
    }

    protected void createAndRegisterBuffer(int remoteLocalId) {
        if(!localBuffers.containsKey(remoteLocalId)) {

            var buffer = dcm.allocRegisteredBuffer(0, DynamicConnectionManager.BUFFER_SIZE);
            buffer.clear();

            registerBuffer(remoteLocalId, buffer);

            var bufferInfo = new BufferInformation(buffer);
            dcm.dch.sendBufferInfo(bufferInfo, dcm.getLocalId(), (short) remoteLocalId);
        }
    }
}
