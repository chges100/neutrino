package de.hhu.bsinfo.neutrino.connection;

import de.hhu.bsinfo.neutrino.buffer.RegisteredBuffer;
import de.hhu.bsinfo.neutrino.verbs.AccessFlag;
import de.hhu.bsinfo.neutrino.verbs.Context;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedList;

public class ConnectionManager {

    private final ArrayList<DeviceContext> deviceContexts;
    private final Deque<RegisteredBuffer> localBuffers;

    public ConnectionManager() throws IOException {

        var deviceCnt = Context.getDeviceCount();
        deviceContexts = new ArrayList<>();

        for(int i = 0; i < deviceCnt; i++) {
            var deviceContext = new DeviceContext(i);
            deviceContexts.add(deviceContext);
        }

        if(deviceContexts.isEmpty()) {
            throw new IOException("No InfiniBand devices could be opened.");
        }

        localBuffers = new LinkedList<>();
    }


    public RegisteredBuffer allocLocalBuffer(int deviceId, int size) {
        var buffer = deviceContexts.get(deviceId).getProtectionDomain().allocateMemory(size, AccessFlag.LOCAL_WRITE, AccessFlag.REMOTE_READ, AccessFlag.REMOTE_WRITE, AccessFlag.MW_BIND);
        localBuffers.add(buffer);

        return buffer;
    }

    public void freeLocalBuffer(RegisteredBuffer buffer) {
        localBuffers.remove(buffer);
        buffer.close();
    }
}
