package de.hhu.bsinfo.neutrino.connection;

import de.hhu.bsinfo.neutrino.buffer.RegisteredBuffer;
import de.hhu.bsinfo.neutrino.connection.connector.SocketConnector;
import de.hhu.bsinfo.neutrino.verbs.AccessFlag;
import de.hhu.bsinfo.neutrino.verbs.Context;
import de.hhu.bsinfo.neutrino.verbs.SendWorkRequest;
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

    private static final ArrayList<DeviceContext> deviceContexts;
    private static final Deque<RegisteredBuffer> localBuffers;
    private static final ArrayList<ReliableConnection> connections;
    private static final ArrayList<UnreliableDatagram> unreliableDatagrams;

    private static final AtomicInteger idCounter = new AtomicInteger();

    static {
        var deviceCnt = Context.getDeviceCount();
        deviceContexts = new ArrayList<>();
        localBuffers = new LinkedList<>();
        connections = new ArrayList<>();
        unreliableDatagrams = new ArrayList<>();


        try {
            for (int i = 0; i < deviceCnt; i++) {
                var deviceContext = new DeviceContext(i);
                deviceContexts.add(deviceContext);
            }
        } catch (IOException e) {
            LOGGER.error("Could not initialize InfiniBand devices");
        }

    }

    public static RegisteredBuffer allocLocalBuffer(DeviceContext deviceContext, long size) {
        LOGGER.info("Allocate new memory region for device {} of size {}", deviceContext.getDeviceId(), size);

        var buffer = deviceContext.getProtectionDomain().allocateMemory(size, AccessFlag.LOCAL_WRITE, AccessFlag.REMOTE_READ, AccessFlag.REMOTE_WRITE, AccessFlag.MW_BIND);
        localBuffers.add(buffer);

        return buffer;
    }

    public static RegisteredBuffer allocLocalBuffer(int deviceId, long size) {
        return allocLocalBuffer(deviceContexts.get(deviceId), size);
    }

    public static void freeLocalBuffer(RegisteredBuffer buffer) {
        LOGGER.info("Free memory region");
        localBuffers.remove(buffer);
        buffer.close();
    }

    public static ReliableConnection createReliableConnection(int deviceId, Socket socket) throws IOException {
        return createReliableConnection(deviceContexts.get(deviceId), socket);

    }

    public static ReliableConnection createUnconnectedReliableConnection(int deviceId) throws IOException {
        return createUnconnectedReliableConnection(deviceContexts.get(deviceId));
    }

    public static ReliableConnection createReliableConnection(DeviceContext deviceContext, Socket socket) throws IOException {
        var connection = createUnconnectedReliableConnection(deviceContext);
        var connector = new SocketConnector(socket, connection);

        connector.connect();

        return connection;
    }

    public static ReliableConnection createReliableConnection(DeviceContext deviceContext, InetSocketAddress remoteAddress) throws IOException {
        return createReliableConnection(deviceContext, new Socket(remoteAddress.getAddress(), remoteAddress.getPort()));
    }

    public static ReliableConnection createReliableConnection(int deviceId, InetSocketAddress remoteAddress) throws IOException {
        return createReliableConnection(deviceContexts.get(deviceId), remoteAddress);
    }

    public static ReliableConnection createUnconnectedReliableConnection(DeviceContext deviceContext) throws IOException {
        var connection = new ReliableConnection(deviceContext);
        connections.add(connection);

        LOGGER.info("Create new reliable connection {}", connection.getConnectionId());

        connection.init();

        return connection;
    }

    public static UnreliableDatagram createUnreliableDatagram(int deviceId) throws IOException {
        return createUnreliableDatagram(deviceContexts.get(deviceId));
    }

    public static UnreliableDatagram createUnreliableDatagram(DeviceContext deviceContext) throws IOException {
        var unreliableDatagram = new UnreliableDatagram(deviceContext);
        unreliableDatagrams.add(unreliableDatagram);

        unreliableDatagram.init();

        return unreliableDatagram;
    }

    public static void closeConnection(ReliableConnection connection) throws IOException {
        LOGGER.info("Close connection {}", connection.getConnectionId());
        connection.close();
        connections.remove(connection);
    }

    public static void closeUnreliableDatagram(UnreliableDatagram unreliableDatagram) throws IOException {
        LOGGER.info("Close Unreliable Datagram");
        unreliableDatagram.close();
        unreliableDatagrams.remove(unreliableDatagram);
    }

    public static int provideConnectionId() {
        return idCounter.getAndIncrement();
    }
}
