package de.hhu.bsinfo.neutrino.connection;

import de.hhu.bsinfo.neutrino.buffer.RegisteredBuffer;
import de.hhu.bsinfo.neutrino.connection.interfaces.Connectable;
import de.hhu.bsinfo.neutrino.connection.interfaces.Executor;
import de.hhu.bsinfo.neutrino.connection.message.Message;
import de.hhu.bsinfo.neutrino.connection.message.MessageType;
import de.hhu.bsinfo.neutrino.connection.util.RCInformation;
import de.hhu.bsinfo.neutrino.verbs.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.StringJoiner;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class ReliableConnection extends QPSocket implements Connectable<RCInformation>, Executor {

    private static final Logger LOGGER = LoggerFactory.getLogger(ReliableConnection.class);
    private static final short LID_MAX = Short.MAX_VALUE;
    private static final int BATCH_SIZE = 10;

    private static final AtomicInteger idCounter = new AtomicInteger(0);
    private final int id;
    private short remoteLid = LID_MAX;

    private final AtomicBoolean isConnected = new AtomicBoolean(false);
    private final AtomicBoolean initConnection = new AtomicBoolean(false);

    public ReliableConnection(DeviceContext deviceContext) throws IOException {

        super(deviceContext);

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

    @Override
    public void connect(RCInformation remoteInfo) throws IOException {

        if(initConnection.getAndSet(true)){
            LOGGER.error("Connection already connected");
            throw new IOException("Connection is already connected");
        }

        if(!queuePair.modify(QueuePair.Attributes.Builder.buildReadyToReceiveAttributesRC(
                remoteInfo.getQueuePairNumber(), remoteInfo.getLocalId(), remoteInfo.getPortNumber()))) {
            throw new IOException("Unable to move queue pair into RTR state");
        }

        remoteLid = remoteInfo.getLocalId();

        LOGGER.info("Moved queue pair into RTR state with remote {}", remoteInfo.getLocalId());

        if(!queuePair.modify(QueuePair.Attributes.Builder.buildReadyToSendAttributesRC())) {
            throw new IOException("Unable to move queue pair into RTS state");
        }

        LOGGER.info("Moved queue pair into RTS state");

        initialHandshake();

        isConnected.getAndSet(true);
    }

    public long send(RegisteredBuffer data) {
        return send(data, 0,  data.capacity());
    }

    public long send(RegisteredBuffer data, long offset, long length) {
        var scatterGatherElement = (ScatterGatherElement) Verbs.getPoolableInstance(ScatterGatherElement.class);
        scatterGatherElement.setAddress(data.getHandle() + offset);
        scatterGatherElement.setLength((int) length);
        scatterGatherElement.setLocalKey(data.getLocalKey());

        var sendWorkRequest = buildSendWorkRequest(scatterGatherElement, sendWrIdProvider.getAndIncrement());

        return postSend(sendWorkRequest);
    }

    protected SendWorkRequest buildSendWorkRequest(ScatterGatherElement sge, int id) {
        return new SendWorkRequest.MessageBuilder(SendWorkRequest.OpCode.SEND, sge).withSendFlags(SendWorkRequest.SendFlag.SIGNALED).withId(id).build();
    }

    @Override
    public long execute(RegisteredBuffer data, SendWorkRequest.OpCode opCode, long offset, long length, long remoteAddress, int remoteKey, long remoteOffset) {
        var scatterGatherElement = (ScatterGatherElement) Verbs.getPoolableInstance(ScatterGatherElement.class);
        scatterGatherElement.setAddress(data.getHandle() + offset);
        scatterGatherElement.setLength((int) length);
        scatterGatherElement.setLocalKey(data.getLocalKey());

        var sendWorkRequest = buildRDMAWorkRequest(opCode, scatterGatherElement, remoteAddress + remoteOffset, remoteKey, sendWrIdProvider.getAndIncrement());

        return postSend(sendWorkRequest);
    }

    protected SendWorkRequest buildRDMAWorkRequest(SendWorkRequest.OpCode opCode, ScatterGatherElement sge, long remoteAddress, int remoteKey, int id) {
        return new SendWorkRequest.RdmaBuilder(opCode, sge, remoteAddress, remoteKey).withSendFlags(SendWorkRequest.SendFlag.SIGNALED).withId(id).build();
    }

    public long receive(RegisteredBuffer data) {
        return receive(data, 0, data.capacity());
    }

    public long receive(RegisteredBuffer data, long offset, long length) {
        var scatterGatherElement = (ScatterGatherElement) Verbs.getPoolableInstance(ScatterGatherElement.class);
        scatterGatherElement.setAddress(data.getHandle() + offset);
        scatterGatherElement.setLength((int) length);
        scatterGatherElement.setLocalKey(data.getLocalKey());

        var receiveWorkRequest = buildReceiveWorkRequest(scatterGatherElement, receiveWrIdProvider.getAndIncrement());
        return postReceive(receiveWorkRequest);
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

    private void initialHandshake() throws IOException{
        LOGGER.debug("Initial handshake of connection {} started", getId());
        var message = new Message(getDeviceContext(), MessageType.RC_HANDSHAKE, "");
        var receiveBuffer = getDeviceContext().allocRegisteredBuffer(Message.getSize());

        receive(receiveBuffer);
        send(message.getByteBuffer());

        int sentCount = 0;
        do{
            sentCount = pollSend(1);
        } while (sentCount == 0);

        int receiveCount = 0;
        do{
            receiveCount = pollReceive(1);
        } while (receiveCount == 0);

        message.close();
        receiveBuffer.close();

        LOGGER.debug("Initial handshake of connection {} finished", getId());
    }

    @Override
    public short disconnect() throws IOException{

        isConnected.getAndSet(false);
        initConnection.getAndSet(false);

        if(remoteLid == LID_MAX) {
            return remoteLid;
        }

        remoteLid = LID_MAX;

        var message = new Message(getDeviceContext(), MessageType.RC_HANDSHAKE, "");
        var receiveBuffer = getDeviceContext().allocRegisteredBuffer(Message.getSize());

        var receiveWrId = receive(receiveBuffer);
        var sendWrId = send(message.getByteBuffer());

        boolean isSent = false;
        while(!isSent) {
            var wcs = pollSendCompletions(BATCH_SIZE);
            for(int i = 0; i < wcs.getLength(); i++) {
                if(wcs.get(i).getId() == sendWrId) {
                    isSent = true;
                }
            }
        }

        boolean isReceived = false;
        while(!isReceived) {
            var wcs = pollSendCompletions(BATCH_SIZE);
            for(int i = 0; i < wcs.getLength(); i++) {
                if(wcs.get(i).getId() == sendWrId) {
                    var msg = new Message(receiveBuffer);
                    if(msg.getMessageType() == MessageType.RC_DISCONNECT)
                        isReceived = true;
                }
            }
        }

        message.close();
        receiveBuffer.close();


        if(!queuePair.modify(QueuePair.Attributes.Builder.buildResetAttributesRC())) {
            throw new IOException("Unable to move queue pair into RESET state");
        }

        init();

        return remoteLid;
    }

    @Override
    public void close() throws IOException {
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

    @Override
    public String toString() {
        return new StringJoiner(", ", UnreliableDatagram.class.getSimpleName() + "[", "]")
                .add("RC id=" + id)
                .add("localId=" + portAttributes.getLocalId())
                .add("portNumber=" + 1)
                .add("queuePairNumber=" + queuePair.getQueuePairNumber())
                .toString();
    }
}
