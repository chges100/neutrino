package de.hhu.bsinfo.neutrino.connection;

import de.hhu.bsinfo.neutrino.verbs.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class QPSocket {
    private static final Logger LOGGER = LoggerFactory.getLogger(QPSocket.class);

    protected final int completionQueueSize = 100;
    protected final int sendQueueSize = 100;
    protected final int receiveQueueSize = 100;

    private final DeviceContext deviceContext;

    protected final CompletionQueue sendCompletionQueue;
    protected final CompletionQueue receiveCompletionQueue;
    protected final PortAttributes portAttributes;

    protected QueuePair queuePair;

    protected final AtomicInteger sendWrIdProvider;
    protected final AtomicInteger receiveWrIdProvider;

    protected QPSocket(DeviceContext deviceContext) throws IOException {

        this.deviceContext = deviceContext;

        portAttributes = deviceContext.getContext().queryPort(1);
        if(portAttributes == null) {
            throw new IOException("Cannot query port");
        }

        sendCompletionQueue = deviceContext.getContext().createCompletionQueue(completionQueueSize);
        if(sendCompletionQueue == null) {
            throw new IOException("Cannot create completion queue");
        }

        receiveCompletionQueue = deviceContext.getContext().createCompletionQueue(completionQueueSize);
        if(receiveCompletionQueue == null) {
            throw new IOException("Cannot create completion queue");
        }

        sendWrIdProvider = new AtomicInteger(0);
        receiveWrIdProvider = new AtomicInteger(0);
    }

    abstract void init() throws IOException;
    abstract void close() throws IOException;

    protected long postSend(SendWorkRequest workRequest) {
        queuePair.postSend(workRequest);

        return workRequest.getId();
    }

    protected long postReceive(ReceiveWorkRequest workRequest) {
        queuePair.postReceive(workRequest);

        return workRequest.getId();
    }

    public DeviceContext getDeviceContext() {
        return deviceContext;
    }

    public PortAttributes getPortAttributes() {
        return portAttributes;
    }

    public QueuePair getQueuePair() {
        return queuePair;
    }
}