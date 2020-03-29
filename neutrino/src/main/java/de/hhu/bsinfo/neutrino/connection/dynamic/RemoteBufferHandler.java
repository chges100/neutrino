package de.hhu.bsinfo.neutrino.connection.dynamic;

import de.hhu.bsinfo.neutrino.buffer.BufferInformation;
import org.agrona.collections.Int2IntHashMap;
import org.agrona.collections.Int2ObjectHashMap;

public class RemoteBufferHandler {
    protected static final int MISSING_VALUE = Integer.MAX_VALUE;
    protected static final int INFO_SENT = 1;
    protected static final int ACKNOWLEDGED = 2;

    private final Int2ObjectHashMap<BufferInformation> remoteBuffers;
    private final Int2IntHashMap remoteBufferAckStatus;
    private final DynamicConnectionManager dcm;

    protected RemoteBufferHandler(DynamicConnectionManager dcm) {
        this.dcm = dcm;

        remoteBuffers = new Int2ObjectHashMap<>();
        remoteBufferAckStatus = new Int2IntHashMap(MISSING_VALUE);
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

    protected void setBufferInfoSent(int remoteLocalId) {
        remoteBufferAckStatus.put(remoteLocalId, INFO_SENT);
    }

    protected void setBufferInfoAck(int remoteLocalId) {
        remoteBufferAckStatus.put(remoteLocalId, ACKNOWLEDGED);
    }

    protected void getBufferInfoStatus(int remoteLocalId) {
        remoteBufferAckStatus.get(remoteLocalId);
    }
}
