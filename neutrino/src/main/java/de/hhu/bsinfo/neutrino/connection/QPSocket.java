package de.hhu.bsinfo.neutrino.connection;

import de.hhu.bsinfo.neutrino.verbs.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public abstract class QPSocket {
    private static final Logger LOGGER = LoggerFactory.getLogger(QPSocket.class);

    protected int sendCompletionQueueSize = 100;
    protected int receiveCompletionQueueSize = 100;

    protected int sendQueueSize = 100;
    protected int receiveQueueSize = 100;

    private final DeviceContext deviceContext;

    protected final CompletionQueue sendCompletionQueue;
    protected final CompletionQueue receiveCompletionQueue;
    protected final PortAttributes portAttributes;

    protected QueuePair queuePair;

    private final AtomicInteger sendQueueFillCount = new AtomicInteger(0);
    private final AtomicInteger receiveQueueFillCount = new AtomicInteger(0);

    protected QPSocket(DeviceContext deviceContext) throws IOException {

        this.deviceContext = deviceContext;

        portAttributes = deviceContext.getContext().queryPort(1);
        if(portAttributes == null) {
            throw new IOException("Cannot query port");
        }

        sendCompletionQueue = deviceContext.getContext().createCompletionQueue(sendCompletionQueueSize);
        if(sendCompletionQueue == null) {
            throw new IOException("Cannot create completion queue");
        }

        receiveCompletionQueue = deviceContext.getContext().createCompletionQueue(receiveCompletionQueueSize);
        if(receiveCompletionQueue == null) {
            throw new IOException("Cannot create completion queue");
        }
    }

    protected QPSocket(DeviceContext deviceContext, int sendQueueSize, int receiveQueueSize, int sendCompletionQueueSize, int receiveCompletionQueueSize) throws IOException {

        this.deviceContext = deviceContext;
        this.sendQueueSize = sendQueueSize;
        this.receiveQueueSize = receiveQueueSize;
        this.sendCompletionQueueSize = sendCompletionQueueSize;
        this.receiveCompletionQueueSize = receiveCompletionQueueSize;

        portAttributes = deviceContext.getContext().queryPort(1);
        if(portAttributes == null) {
            throw new IOException("Cannot query port");
        }

        sendCompletionQueue = deviceContext.getContext().createCompletionQueue(sendCompletionQueueSize);
        if(sendCompletionQueue == null) {
            throw new IOException("Cannot create completion queue");
        }

        receiveCompletionQueue = deviceContext.getContext().createCompletionQueue(receiveCompletionQueueSize);
        if(receiveCompletionQueue == null) {
            throw new IOException("Cannot create completion queue");
        }
    }

    protected QPSocket(DeviceContext deviceContext, int sendQueueSize, int receiveQueueSize, CompletionQueue sendCompletionQueue, CompletionQueue receiveCompletionQueue) throws IOException {

        this.deviceContext = deviceContext;
        this.sendQueueSize = sendQueueSize;
        this.receiveQueueSize = receiveQueueSize;

        portAttributes = deviceContext.getContext().queryPort(1);
        if(portAttributes == null) {
            throw new IOException("Cannot query port");
        }

        this.sendCompletionQueue = sendCompletionQueue;
        this.receiveCompletionQueue = receiveCompletionQueue;

        this.sendCompletionQueueSize = sendCompletionQueue.getMaxElements();
        this.receiveCompletionQueueSize = receiveCompletionQueue.getMaxElements();
    }

    abstract void init() throws IOException;
    abstract void close() throws IOException;

    protected void postSend(SendWorkRequest workRequest) {
        boolean posted = false;

        while (!posted) {
            posted = tryPostSend(workRequest);
        }
    }

    protected void postReceive(ReceiveWorkRequest workRequest) {
        boolean posted = false;

        while (!posted) {
            posted = tryPostReceive(workRequest);
        }
    }

    protected boolean tryPostSend(SendWorkRequest workRequest) {
        int oldVal = sendQueueFillCount.get();

        if(oldVal < sendQueueSize) {
            int newVal = oldVal + 1;
            if(sendQueueFillCount.compareAndSet(oldVal, newVal)) {
                return queuePair.postSend(workRequest);
            }
        }

        return false;
    }

    protected boolean tryPostReceive(ReceiveWorkRequest workRequest) {
        int oldVal = receiveQueueFillCount.get();

        if(oldVal < receiveQueueSize) {
            int newVal = oldVal + 1;
            if(receiveQueueFillCount.compareAndSet(oldVal, newVal)) {
                return queuePair.postReceive(workRequest);
            }
        }

        return false;
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

    public void acknowledgeSendCompletion() {
        sendQueueFillCount.decrementAndGet();
    }

    public void acknowledgeReceiveCompletion() {
        receiveQueueFillCount.decrementAndGet();
    }

    public int getSendQueueSize() {
        return sendQueueSize;
    }

    public int getReceiveQueueSize() {
        return receiveQueueSize;
    }
}