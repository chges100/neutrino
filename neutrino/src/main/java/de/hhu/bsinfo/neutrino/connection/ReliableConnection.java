package de.hhu.bsinfo.neutrino.connection;

import de.hhu.bsinfo.neutrino.buffer.LocalBuffer;
import de.hhu.bsinfo.neutrino.buffer.RegisteredBuffer;
import de.hhu.bsinfo.neutrino.verbs.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Deque;
import java.util.LinkedList;

public class ReliableConnection implements Connection{

    private static final Logger LOGGER = LoggerFactory.getLogger(ReliableConnection.class);

    private final int completionQueueSize = 100;
    private final int sendQueueSize = 100;
    private final int receiveQueueSize = 100;

    private final Context context;
    private final Deque<RegisteredBuffer> localBuffers;
    private final ProtectionDomain protectionDomain;
    private final QueuePair queuePair;
    private final CompletionQueue sendCompletionQueue;
    private final CompletionQueue receiveCompletionQueue;
    private final CompletionChannel completionChannel;

    public ReliableConnection() throws IOException {
        var deviceCnt = Context.getDeviceCount();
        if(0 < deviceCnt) {
            context = Context.openDevice(0);
            if(context == null) {
                throw  new IOException("Cannot open Context");
            }
        } else {
            throw new IOException("No InfiniBand devices available");
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

        queuePair = protectionDomain.createQueuePair(new QueuePair.InitialAttributes.Builder(
                QueuePair.Type.RC, sendCompletionQueue, receiveCompletionQueue, sendQueueSize, receiveQueueSize, 1, 1).build());
        if(queuePair == null) {
            throw new IOException("Cannot create queue pair");
        }

        localBuffers = new LinkedList<>();
    }

    @Override
    public void init() throws IOException {
        if(!queuePair.modify(QueuePair.Attributes.Builder.buildInitAttributesRC((short) 0, (byte) 1, AccessFlag.LOCAL_WRITE, AccessFlag.REMOTE_READ, AccessFlag.REMOTE_WRITE))) {
            throw new IOException("Unable to move queue pair into INIT state");
        }
    }

    @Override
    public void connect(Socket socket) throws IOException {

    }

    @Override
    public void send(RegisteredBuffer data) throws IOException {

    }

    @Override
    public void receive() throws IOException {

    }

    @Override
    public void close() throws IOException {

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


}
