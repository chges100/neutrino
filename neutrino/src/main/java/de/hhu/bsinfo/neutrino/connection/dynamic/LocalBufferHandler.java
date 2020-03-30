package de.hhu.bsinfo.neutrino.connection.dynamic;

import de.hhu.bsinfo.neutrino.buffer.BufferInformation;
import de.hhu.bsinfo.neutrino.buffer.RegisteredBuffer;
import org.agrona.collections.Int2IntHashMap;
import org.agrona.collections.Int2ObjectHashMap;

public class LocalBufferHandler {
    protected static final int MISSING_VALUE = Integer.MAX_VALUE;
    protected static final int INFO_SENT = 1;
    protected static final int ACKNOWLEDGED = 2;

    private final Int2ObjectHashMap<RegisteredBuffer> localBuffers;
    private final Int2IntHashMap remoteBufferAckStatus;
    private final DynamicConnectionManager dcm;

    protected LocalBufferHandler(DynamicConnectionManager dcm) {
        this.dcm = dcm;

        localBuffers = new Int2ObjectHashMap<>();
        remoteBufferAckStatus = new Int2IntHashMap(MISSING_VALUE);
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

    protected void setBufferInfoSent(int remoteLocalId) {
        remoteBufferAckStatus.put(remoteLocalId, INFO_SENT);
    }

    protected void setBufferInfoAck(int remoteLocalId) {
        remoteBufferAckStatus.put(remoteLocalId, ACKNOWLEDGED);
    }

    protected int getBufferInfoStatus(int remoteLocalId) {
        return remoteBufferAckStatus.get(remoteLocalId);
    }
}
