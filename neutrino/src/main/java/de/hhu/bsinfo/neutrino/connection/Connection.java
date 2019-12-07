package de.hhu.bsinfo.neutrino.connection;

import de.hhu.bsinfo.neutrino.buffer.RegisteredBuffer;
import de.hhu.bsinfo.neutrino.verbs.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.Socket;
import java.util.Deque;
import java.util.LinkedList;

public abstract class Connection {

    private static final Logger LOGGER = LoggerFactory.getLogger(Connection.class);

    private final int completionQueueSize = 100;
    private final int sendQueueSize = 100;
    private final int receiveQueueSize = 100;

    private final Context context;
    private final Deque<RegisteredBuffer> localBuffers;
    private final ProtectionDomain protectionDomain;
    private final CompletionQueue sendCompletionQueue;
    private final CompletionQueue receiveCompletionQueue;
    private final CompletionChannel completionChannel;
    private final PortAttributes portAttributes;


    Connection(int deviceNumber) throws IOException {
        var deviceCnt = Context.getDeviceCount();
        if(deviceNumber < deviceCnt) {
            context = Context.openDevice(0);
            if(context == null) {
                throw  new IOException("Cannot open Context");
            }
        } else {
            throw new IOException("InfniniBand device unavailable");
        }

        portAttributes = context.queryPort(1);
        if(portAttributes == null) {
            throw new IOException("Cannot query port");
        }

        protectionDomain = context.allocateProtectionDomain();
        if(protectionDomain == null) {
            throw  new IOException("Unable to allocate protection domain");
        }

        completionChannel = context.createCompletionChannel();
        if(completionChannel == null) {
            throw  new IOException("Cannot create completion channel");
        }

        sendCompletionQueue = context.createCompletionQueue(completionQueueSize);
        if(sendCompletionQueue == null) {
            throw new IOException("Cannot create completion queue");
        }

        receiveCompletionQueue = context.createCompletionQueue(completionQueueSize);
        if(receiveCompletionQueue == null) {
            throw new IOException("Cannot create completion queue");
        }

        localBuffers = new LinkedList<>();
    }

    abstract void init() throws IOException;

    abstract void connect(Socket socket) throws IOException;

    abstract void send(RegisteredBuffer data) throws IOException;

    abstract void receive() throws IOException;

    void close() throws IOException {
        protectionDomain.close();
        context.close();
    }

    public RegisteredBuffer getLocalBuffer(int size) {
        var buffer = protectionDomain.allocateMemory(size, AccessFlag.LOCAL_WRITE, AccessFlag.REMOTE_READ, AccessFlag.REMOTE_WRITE, AccessFlag.MW_BIND);
        localBuffers.add(buffer);

        return buffer;
    }

    public void freeLocalBuffer(RegisteredBuffer buffer) {
        localBuffers.remove(buffer);
        buffer.close();
    }

    public int getCompletionQueueSize() {
        return completionQueueSize;
    }

    public Context getContext() {
        return context;
    }

    public Deque<RegisteredBuffer> getLocalBuffers() {
        return localBuffers;
    }

    public ProtectionDomain getProtectionDomain() {
        return protectionDomain;
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

}
