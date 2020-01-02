package de.hhu.bsinfo.neutrino.connection;

import de.hhu.bsinfo.neutrino.buffer.RegisteredBuffer;
import de.hhu.bsinfo.neutrino.verbs.AccessFlag;
import de.hhu.bsinfo.neutrino.verbs.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicInteger;

public class ConnectionManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionManager.class);

    private final ArrayList<DeviceContext> deviceContexts;
    private final Deque<RegisteredBuffer> localBuffers;
    private final ArrayList<Connection> connections;

    private static final AtomicInteger idCounter = new AtomicInteger();

    public ConnectionManager() throws IOException {

        var deviceCnt = Context.getDeviceCount();
        deviceContexts = new ArrayList<>();
        localBuffers = new LinkedList<>();
        connections = new ArrayList<>();

        for(int i = 0; i < deviceCnt; i++) {
            var deviceContext = new DeviceContext(i);
            deviceContexts.add(deviceContext);
        }

        if(deviceContexts.isEmpty()) {
            throw new IOException("No InfiniBand devices could be opened");
        }


    }

    public RegisteredBuffer allocLocalBuffer(DeviceContext deviceContext, int size) {
        LOGGER.info("Allocate new memory region for device {}", deviceContext.getDeviceId());

        var buffer = deviceContext.getProtectionDomain().allocateMemory(size, AccessFlag.LOCAL_WRITE, AccessFlag.REMOTE_READ, AccessFlag.REMOTE_WRITE, AccessFlag.MW_BIND);
        localBuffers.add(buffer);

        return buffer;
    }

    public RegisteredBuffer allocLocalBuffer(int deviceId, int size) {
        return allocLocalBuffer(deviceContexts.get(deviceId), size);
    }

    public void freeLocalBuffer(RegisteredBuffer buffer) {
        LOGGER.info("Free memory region");
        localBuffers.remove(buffer);
        buffer.close();
    }

    public ReliableConnection createReliableConnection(int deviceId, Socket socket) throws IOException {
        var connection = new ReliableConnection(deviceContexts.get(deviceId));
        connections.add(connection);

        LOGGER.info("Create new reliable connection {}", connection.getConnectionId());

        connection.init();

        connection.connect(socket);

        return connection;
    }

    public void closeConnection(Connection connection) throws IOException {
        LOGGER.info("Close connection {}", connection.getConnectionId());
        connection.close();
        connections.remove(connection);
    }

    public static int provideConnectionId() {
        return idCounter.getAndIncrement();
    }
}
