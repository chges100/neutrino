package de.hhu.bsinfo.neutrino.connection;

import de.hhu.bsinfo.neutrino.buffer.RegisteredBuffer;
import de.hhu.bsinfo.neutrino.verbs.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.Socket;

public abstract class Connection {

    private static final Logger LOGGER = LoggerFactory.getLogger(Connection.class);

    private final int completionQueueSize = 100;
    private final int sendQueueSize = 100;
    private final int receiveQueueSize = 100;

    private final DeviceContext deviceContext;

    private final int connectionId;

    private final CompletionQueue sendCompletionQueue;
    private final CompletionQueue receiveCompletionQueue;
    private final CompletionChannel completionChannel;
    private final PortAttributes portAttributes;


    Connection(DeviceContext deviceContext) throws IOException {

        this.deviceContext = deviceContext;
        this.connectionId = ConnectionManager.provideConnectionId();

        portAttributes = deviceContext.getContext().queryPort(1);
        if(portAttributes == null) {
            throw new IOException("Cannot query port");
        }

        completionChannel = deviceContext.getContext().createCompletionChannel();
        if(completionChannel == null) {
            throw  new IOException("Cannot create completion channel");
        }

        sendCompletionQueue = deviceContext.getContext().createCompletionQueue(completionQueueSize);
        if(sendCompletionQueue == null) {
            throw new IOException("Cannot create completion queue");
        }

        receiveCompletionQueue = deviceContext.getContext().createCompletionQueue(completionQueueSize);
        if(receiveCompletionQueue == null) {
            throw new IOException("Cannot create completion queue");
        }
    }

    abstract void init() throws IOException;

    abstract void connect(Socket socket) throws IOException;

    void close() throws IOException {
    }

    public int getCompletionQueueSize() {
        return completionQueueSize;
    }

    public CompletionQueue getSendCompletionQueue() {
        return sendCompletionQueue;
    }

    public CompletionQueue getReceiveCompletionQueue() {
        return receiveCompletionQueue;
    }

    public CompletionChannel getCompletionChannel() {
        return completionChannel;
    }

    public PortAttributes getPortAttributes() {
        return portAttributes;
    }

    public int getSendQueueSize() {
        return sendQueueSize;
    }

    public int getReceiveQueueSize() {
        return receiveQueueSize;
    }

    public DeviceContext getDeviceContext() {
        return deviceContext;
    }

    public int getConnectionId() {
        return connectionId;
    }
}
