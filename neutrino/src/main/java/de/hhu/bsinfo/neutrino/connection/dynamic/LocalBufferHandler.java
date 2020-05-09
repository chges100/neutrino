package de.hhu.bsinfo.neutrino.connection.dynamic;

import de.hhu.bsinfo.neutrino.buffer.BufferInformation;
import de.hhu.bsinfo.neutrino.buffer.RegisteredBuffer;
import org.jctools.maps.NonBlockingHashMapLong;

public class LocalBufferHandler {

    private final NonBlockingHashMapLong<RegisteredBuffer> localBuffers = new NonBlockingHashMapLong<>();
    private final DynamicConnectionManager dcm;

    protected LocalBufferHandler(DynamicConnectionManager dcm) {
        this.dcm = dcm;
    }

    protected RegisteredBuffer getBuffer(short remoteLocalId) {
        return localBuffers.get(remoteLocalId);
    }

    protected boolean hasBuffer(short remoteLocalId) {
        return localBuffers.containsKey(remoteLocalId);
    }

    protected void createAndRegisterBuffer(short remoteLocalId) {

        var buffer = dcm.allocRegisteredBuffer(0, dcm.rdmaBufferSize);
        buffer.clear();

        var ret = localBuffers.putIfAbsent(remoteLocalId, buffer);

        if (ret == null) {
            var bufferInfo = new BufferInformation(buffer);
            dcm.dch.sendBufferInfo(bufferInfo, dcm.getLocalId(), (short) remoteLocalId);
        } else {
            buffer.close();
        }
    }
}
