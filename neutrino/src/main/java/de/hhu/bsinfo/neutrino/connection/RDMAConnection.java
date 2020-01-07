package de.hhu.bsinfo.neutrino.connection;

import de.hhu.bsinfo.neutrino.buffer.RemoteBuffer;
import de.hhu.bsinfo.neutrino.connection.message.Message;
import de.hhu.bsinfo.neutrino.connection.message.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.Socket;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class RDMAConnection  extends ReliableConnection{
    private static final Logger LOGGER = LoggerFactory.getLogger(RDMAConnection.class);

    private final ReliableConnection communicationChannel;
    private final HashMap<Integer, RemoteBuffer> remoteBuffers;
    private final AtomicInteger allocCounter;

    private static final ConnectionType connectionType = ConnectionType.RDMAConnection;

    public RDMAConnection(DeviceContext deviceContext) throws IOException {
        super(deviceContext);

        communicationChannel = ConnectionManager.createUnconnectedReliableConnection(getDeviceContext());
        remoteBuffers = new HashMap<>();
        allocCounter = new AtomicInteger(0);
    }

    @Override
    public void connect(Socket socket) throws IOException {
        super.connect(socket);

        communicationChannel.connect(socket);
    }

    public RemoteBuffer allocRemoteBuffer(int size) {
        int id = allocCounter.getAndIncrement();
        Message msg = new Message(this, MessageType.REMOTE_BUF_ALLOC, id + ":" + size);
        // TODO
        var remoteBuffer = new RemoteBuffer(getQueuePair(), 0, 0, 0);

        return null;
    }

    public int pollForNewRemoteBuffers() {
        // TODO
        return 0;
    }

    public static ConnectionType getConnectionType() {
        return connectionType;
    }
}

