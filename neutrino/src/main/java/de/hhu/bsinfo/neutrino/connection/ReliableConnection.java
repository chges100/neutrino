package de.hhu.bsinfo.neutrino.connection;

import de.hhu.bsinfo.neutrino.buffer.RegisteredBuffer;
import de.hhu.bsinfo.neutrino.connection.interfaces.Connectable;
import de.hhu.bsinfo.neutrino.connection.message.Message;
import de.hhu.bsinfo.neutrino.connection.message.MessageType;
import de.hhu.bsinfo.neutrino.connection.util.RCInformation;
import de.hhu.bsinfo.neutrino.verbs.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.StringJoiner;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.StampedLock;

public class ReliableConnection extends QPSocket implements Connectable<Boolean, RCInformation> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ReliableConnection.class);
    private static final short INVALID_LID = Short.MAX_VALUE;
    private static final int BATCH_SIZE = 10;
    private static final long HANDSHAKE_CONNECT_TIMEOUT = 2000;
    private static final long HANDSHAKE_DISCONNECT_TIMEOUT = 200;
    private static final int HANDSHAKE_POLL_QUEUE_TIMEOUT = 40;

    private static final AtomicInteger idCounter = new AtomicInteger(0);
    private final int id;
    private AtomicInteger remoteLid = new AtomicInteger(INVALID_LID);

    private final AtomicBoolean isConnected = new AtomicBoolean(false);
    private final AtomicBoolean changeConnection = new AtomicBoolean(false);

    private final RCHandshakeQueue handshakeQueue;

    public ReliableConnection(DeviceContext deviceContext) throws IOException {

        super(deviceContext);

        handshakeQueue = new RCHandshakeQueue();

        id = idCounter.getAndIncrement();
        LOGGER.info("Create reliable connection with id {}", id);

        queuePair = deviceContext.getProtectionDomain().createQueuePair(new QueuePair.InitialAttributes.Builder(
                QueuePair.Type.RC, sendCompletionQueue, receiveCompletionQueue, sendQueueSize, receiveQueueSize, 1, 1).build());
        if(queuePair == null) {
            throw new IOException("Cannot create queue pair");
        }
    }

    public ReliableConnection(DeviceContext deviceContext, int sendQueueSize, int receiveQueueSize, int sendCompletionQueueSize, int receiveCompletionQueueSize) throws IOException {

        super(deviceContext,sendQueueSize, receiveQueueSize, sendCompletionQueueSize, receiveCompletionQueueSize);

        handshakeQueue = new RCHandshakeQueue();

        id = idCounter.getAndIncrement();
        LOGGER.info("Create reliable connection with id {}", id);

        queuePair = deviceContext.getProtectionDomain().createQueuePair(new QueuePair.InitialAttributes.Builder(
                QueuePair.Type.RC, sendCompletionQueue, receiveCompletionQueue, sendQueueSize, receiveQueueSize, 1, 1).build());
        if(queuePair == null) {
            throw new IOException("Cannot create queue pair");
        }
    }

    public ReliableConnection(DeviceContext deviceContext, int sendQueueSize, int receiveQueueSize, CompletionQueue sendCompletionQueue, CompletionQueue receiveCompletionQueue) throws IOException {

        super(deviceContext, sendQueueSize, receiveQueueSize, sendCompletionQueue, receiveCompletionQueue);

        handshakeQueue = new RCHandshakeQueue();

        id = idCounter.getAndIncrement();
        LOGGER.info("Create reliable connection with id {}", id);

        queuePair = deviceContext.getProtectionDomain().createQueuePair(new QueuePair.InitialAttributes.Builder(
                QueuePair.Type.RC, sendCompletionQueue, receiveCompletionQueue, sendQueueSize, receiveQueueSize, 1, 1).build());
        if(queuePair == null) {
            throw new IOException("Cannot create queue pair");
        }
    }

    @Override
    public void init() throws IOException {
        if(!queuePair.modify(QueuePair.Attributes.Builder.buildInitAttributesRC((short) 0, (byte) 1, AccessFlag.LOCAL_WRITE, AccessFlag.REMOTE_READ, AccessFlag.REMOTE_WRITE))) {
            throw new IOException("Unable to move queue pair into INIT state");
        }
    }

    public void reset() throws IOException {
        if(!queuePair.modify(QueuePair.Attributes.Builder.buildResetAttributesRC())) {
            throw new IOException("Unable to move queue pair into RESET state");
        }
    }

    @Override
    public Boolean connect(RCInformation remoteInfo) throws IOException {

        if(changeConnection.getAndSet(true)){
            return isConnected.get();
        }

        if(!queuePair.modify(QueuePair.Attributes.Builder.buildReadyToReceiveAttributesRC(
                remoteInfo.getQueuePairNumber(), remoteInfo.getLocalId(), remoteInfo.getPortNumber()))) {
            throw new IOException("Unable to move queue pair into RTR state");
        }

        LOGGER.info("Moved queue pair into RTR state with remote {}", remoteInfo.getLocalId());

        if(!queuePair.modify(QueuePair.Attributes.Builder.buildReadyToSendAttributesRC())) {
            throw new IOException("Unable to move queue pair into RTS state");
        }

        LOGGER.info("Moved queue pair into RTS state");

        if(!handshake(MessageType.RC_CONNECT, HANDSHAKE_CONNECT_TIMEOUT)) {
            reset();
            init();

            changeConnection.set(false);
            throw  new IOException("Connection " + id + ": Could not finish initial handshake to remote " + remoteInfo.getLocalId());
        }

        remoteLid.set(remoteInfo.getLocalId());
        isConnected.getAndSet(true);

        return true;
    }

    public long send(RegisteredBuffer data) throws IOException  {
        return send(data, 0,  data.capacity());
    }

    public long send(RegisteredBuffer data, long offset, long length) throws IOException {
        if (isConnected.get()) {
             return internalSend(data, offset, length);
        } else {
            throw new IOException("Could not send data - RC is not connected yet.");
        }
    }

    private long internalSend(RegisteredBuffer data, long offset, long length) {
        var scatterGatherElement = (ScatterGatherElement) Verbs.getPoolableInstance(ScatterGatherElement.class);
        scatterGatherElement.setAddress(data.getHandle() + offset);
        scatterGatherElement.setLength((int) length);
        scatterGatherElement.setLocalKey(data.getLocalKey());

        var sendWorkRequest = buildSendWorkRequest(scatterGatherElement, sendWrIdProvider.getAndIncrement());

        var wrId = postSend(sendWorkRequest);

        scatterGatherElement.releaseInstance();
        sendWorkRequest.releaseInstance();

        return wrId;
    }

    protected SendWorkRequest buildSendWorkRequest(ScatterGatherElement sge, int id) {
        return new SendWorkRequest.MessageBuilder(SendWorkRequest.OpCode.SEND, sge).withSendFlags(SendWorkRequest.SendFlag.SIGNALED).withId(id).build();
    }

    public long execute(RegisteredBuffer data, SendWorkRequest.OpCode opCode, long offset, long length, long remoteAddress, int remoteKey, long remoteOffset) throws IOException  {
        if(isConnected.get()) {
            return internalExecute(data, opCode, offset, length, remoteAddress, remoteKey, remoteOffset);
        } else {
            throw new IOException("Could not execute on remote - RC is not connected yet.");
        }
    }

    private long internalExecute(RegisteredBuffer data, SendWorkRequest.OpCode opCode, long offset, long length, long remoteAddress, int remoteKey, long remoteOffset) {
        var scatterGatherElement = (ScatterGatherElement) Verbs.getPoolableInstance(ScatterGatherElement.class);
        scatterGatherElement.setAddress(data.getHandle() + offset);
        scatterGatherElement.setLength((int) length);
        scatterGatherElement.setLocalKey(data.getLocalKey());

        var sendWorkRequest = buildRDMAWorkRequest(opCode, scatterGatherElement, remoteAddress + remoteOffset, remoteKey, sendWrIdProvider.getAndIncrement());

        var wrId = postSend(sendWorkRequest);

        scatterGatherElement.releaseInstance();
        sendWorkRequest.releaseInstance();

        return wrId;
    }

    protected SendWorkRequest buildRDMAWorkRequest(SendWorkRequest.OpCode opCode, ScatterGatherElement sge, long remoteAddress, int remoteKey, int id) {
        return new SendWorkRequest.RdmaBuilder(opCode, sge, remoteAddress, remoteKey).withSendFlags(SendWorkRequest.SendFlag.SIGNALED).withId(id).build();
    }

    public long receive(RegisteredBuffer data) throws IOException {
        return receive(data, 0, data.capacity());
    }

    public long receive(RegisteredBuffer data, long offset, long length) throws IOException {
        if(isConnected.get()) {
            return internalReceive(data, offset, length);
        } else {
            throw new IOException("Could not receive - RC is not connected yet.");
        }
    }

    private long internalReceive(RegisteredBuffer data, long offset, long length) {
        var scatterGatherElement = (ScatterGatherElement) Verbs.getPoolableInstance(ScatterGatherElement.class);
        scatterGatherElement.setAddress(data.getHandle() + offset);
        scatterGatherElement.setLength((int) length);
        scatterGatherElement.setLocalKey(data.getLocalKey());

        var receiveWorkRequest = buildReceiveWorkRequest(scatterGatherElement, receiveWrIdProvider.getAndIncrement());

        var wrId = postReceive(receiveWorkRequest);

        scatterGatherElement.releaseInstance();
        receiveWorkRequest.releaseInstance();

        return wrId;
    }

    protected ReceiveWorkRequest buildReceiveWorkRequest(ScatterGatherElement sge, int id) {
        return new ReceiveWorkRequest.Builder().withScatterGatherElement(sge).withId(id).build();
    }

    private CompletionQueue.WorkCompletionArray pollReceiveCompletions(int count) {
        var completionArray = new CompletionQueue.WorkCompletionArray(count);
        receiveCompletionQueue.poll(completionArray);

        return completionArray;
    }

    public int pollReceive(int count) throws IOException {
        var completionArray = pollReceiveCompletions(count);

        for(int i = 0; i < completionArray.getLength(); i++) {
            var completion = completionArray.get(i);
            if(completion.getStatus() != WorkCompletion.Status.SUCCESS) {
                LOGGER.error("Work completion failes with error [{}]: {}", completion.getStatus(), completion.getStatusMessage());
                throw new IOException("WorkCompletion Failed");
            }
        }

        return completionArray.getLength();
    }

    private CompletionQueue.WorkCompletionArray pollSendCompletions(int count) {
        var completionArray = new CompletionQueue.WorkCompletionArray(count);
        sendCompletionQueue.poll(completionArray);


        return completionArray;
    }

    public int pollSend(int count) throws IOException {
        var completionArray = pollSendCompletions(count);

        for(int i = 0; i < completionArray.getLength(); i++) {
            var completion = completionArray.get(i);
            if(completion.getStatus() != WorkCompletion.Status.SUCCESS) {
                LOGGER.error("Work completion failes with error [{}]: {}", completion.getStatus(), completion.getStatusMessage());
                throw new IOException("WorkCompletion Failed");
            }
        }

        return completionArray.getLength();
    }

    public void handleSendError() throws IOException {
        if(queuePair.getState() == QueuePair.State.SQE) {

            if(!queuePair.modify(QueuePair.Attributes.Builder.buildReadyToSendAttributesRC())) {
                throw new IOException("Unable to move queue pair into RTS state");
            }

            LOGGER.info("Recorvered queue pair from SQE into RTS state");
        }
    }

    public void resetFromError() throws IOException {
        if(queuePair.getState() == QueuePair.State.ERR) {
            reset();
            init();
        }
    }

    private boolean handshake(MessageType msgType, long timeOut) throws IOException {
        LOGGER.info("Handshake of connection {} with message {} started", getId(), msgType);

        var message = new Message(getDeviceContext(), msgType);
        var receiveBuffer = getDeviceContext().allocRegisteredBuffer(Message.getSize());
        receiveBuffer.clear();


        var receiveWrId = internalReceive(receiveBuffer, 0, receiveBuffer.capacity());
        long sendWrId = 0;

        boolean isTimeOut = false;


        boolean isSent = false;
        boolean isReceived = false;
        var startTime = System.currentTimeMillis();

        internalSend(message.getByteBuffer(), 0, message.getNativeSize());

        while(!(isSent && isReceived) && !isTimeOut) {

            if(System.currentTimeMillis() - startTime > timeOut) {
                isTimeOut = true;
            } else {
                try {
                    var value = handshakeQueue.poll(HANDSHAKE_POLL_QUEUE_TIMEOUT, TimeUnit.MILLISECONDS);

                    if(value == RCHandshakeQueue.SEND_COMPLETE) {
                        isSent = true;
                    } else if(value == RCHandshakeQueue.RECEIVE_COMPLETE) {
                        isReceived = true;
                    } else if(value == RCHandshakeQueue.SEND_ERROR) {
                        internalSend(message.getByteBuffer(), 0, message.getNativeSize());
                    }
                } catch (Exception e) {
                    LOGGER.trace("Exception accured during polling: {}", e);
                }
            }
        }


        if(!isTimeOut) {
            LOGGER.info("Handshake of connection {}  with message {} finished", getId(), msgType);
        }

        message.close();
        receiveBuffer.close();

        return !isTimeOut;
    }

    @Override
    public void disconnect() throws IOException{

        // either another thread is alreading disconnecting this connection or it is in unconnected state
        if(!isConnected.getAndSet(false)) {
            throw new IOException("Connection already disconnecting or disconnected");
        }

        LOGGER.debug("Start to disconnect connection {} from {}", id, remoteLid);

        // set remote LID
        remoteLid.getAndSet(INVALID_LID);

        handshake(MessageType.RC_DISCONNECT, HANDSHAKE_DISCONNECT_TIMEOUT);

        reset();
        init();

        // now connection is ready to be connected
        changeConnection.getAndSet(false);

        LOGGER.info("Disconnected connection {}", id);
    }

    @Override
    public void close() throws IOException {
        reset();
        LOGGER.info("Close reliable connection {}", id);
        queuePair.close();
    }

    public int getId() {
        return id;
    }

    public QueuePair getQueuePair() {
        return queuePair;
    }

    public boolean isConnected() {
        return isConnected.get();
    }

    public short getRemoteLocalId() {
        return (short)remoteLid.get();
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", UnreliableDatagram.class.getSimpleName() + "[", "]")
                .add("RC id=" + id)
                .add("localId=" + portAttributes.getLocalId())
                .add("portNumber=" + 1)
                .add("queuePairNumber=" + queuePair.getQueuePairNumber())
                .toString();
    }

    public RCHandshakeQueue getHandshakeQueue() {
        return handshakeQueue;
    }


    public class RCHandshakeQueue  {
        protected static final int SEND_COMPLETE = 0;
        protected static final int RECEIVE_COMPLETE = 1;
        protected static final int SEND_ERROR = 2;

        protected final ArrayBlockingQueue<Integer> queue;

        private static final int CAPACITY = 5;

        public RCHandshakeQueue() {
            this.queue = new ArrayBlockingQueue<>(CAPACITY);
        }

        public void pushSendComplete() {
            queue.add(SEND_COMPLETE);
        }

        public void pushReceiveComplete() {
            queue.add(RECEIVE_COMPLETE);
        }

        public void pushSendError() {
            queue.add(SEND_ERROR);
        }

        public int poll(int timeout, TimeUnit unit) throws InterruptedException {
            return queue.poll(timeout, unit);
        }

        public int poll() {
            return queue.poll();
        }

    }
}
