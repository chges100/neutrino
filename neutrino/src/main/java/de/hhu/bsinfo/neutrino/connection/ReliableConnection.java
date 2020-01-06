package de.hhu.bsinfo.neutrino.connection;

import de.hhu.bsinfo.neutrino.buffer.RegisteredBuffer;
import de.hhu.bsinfo.neutrino.connection.message.Message;
import de.hhu.bsinfo.neutrino.verbs.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;

public class ReliableConnection extends Connection{

    private static final Logger LOGGER = LoggerFactory.getLogger(ReliableConnection.class);

    private static final ConnectionType connectionType = ConnectionType.ReliableConnection;

    private final QueuePair queuePair;
    private final AtomicBoolean isConnected = new AtomicBoolean(false);


    public ReliableConnection(DeviceContext deviceContext) throws IOException {

        super(deviceContext);

        queuePair = deviceContext.getProtectionDomain().createQueuePair(new QueuePair.InitialAttributes.Builder(
                QueuePair.Type.RC, getSendCompletionQueue(), getReceiveCompletionQueue(), getSendQueueSize(), getReceiveQueueSize(), 1, 1).build());
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
    public void connect(Socket socket) throws IOException {

        if(isConnected.getAndSet(true)){
            LOGGER.error("Connection already connected");
            throw new IOException("Connection is already connected");
        }

        var localInfo = new ConnectionInformation((byte) 1, getPortAttributes().getLocalId(), queuePair.getQueuePairNumber());

        LOGGER.info("Local connection information: {}", localInfo);

        socket.getOutputStream().write(ByteBuffer.allocate(Byte.BYTES + Short.BYTES + Integer.BYTES)
                .put(localInfo.getPortNumber())
                .putShort(localInfo.getLocalId())
                .putInt(localInfo.getQueuePairNumber())
                .array());

        LOGGER.info("Waiting for remote connection information");

        var byteBuffer = ByteBuffer.wrap(socket.getInputStream().readNBytes(Byte.BYTES + Short.BYTES + Integer.BYTES));
        var remoteInfo = new ConnectionInformation(byteBuffer);

        LOGGER.info("Received connection information: {}", remoteInfo);

        if(!queuePair.modify(QueuePair.Attributes.Builder.buildReadyToReceiveAttributesRC(
                remoteInfo.getQueuePairNumber(), remoteInfo.getLocalId(), remoteInfo.getPortNumber()))) {
            throw new IOException("Unable to move queue pair into RTR state");
        }

        LOGGER.info("Moved queue pair into RTR state");

        if(!queuePair.modify(QueuePair.Attributes.Builder.buildReadyToSendAttributesRC())) {
            throw new IOException("Unable to move queue pair into RTS state");
        }

        LOGGER.info("Moved queue pair into RTS state");
    }

    public long send(RegisteredBuffer data) {
        return send(data, 0, (int) data.capacity());
    }

    public long send(RegisteredBuffer data, int offset, int length) {
        var scatterGatherElement = (ScatterGatherElement) Verbs.getPoolableInstance(ScatterGatherElement.class);
        scatterGatherElement.setAddress(data.getHandle() + offset);
        scatterGatherElement.setLength(length);
        scatterGatherElement.setLocalKey(data.getLocalKey());

        var sendWorkRequest = new SendWorkRequest.MessageBuilder(SendWorkRequest.OpCode.SEND, scatterGatherElement).withSendFlags(SendWorkRequest.SendFlag.SIGNALED).build();

        queuePair.postSend(sendWorkRequest);

        return sendWorkRequest.getId();
    }

    public long sendMessage(Message message) {
        return send(message.getByteBuffer());
    }

    public long receive(RegisteredBuffer data) {
        return receive(data, 0, (int) data.capacity());
    }

    public long receive(RegisteredBuffer data, int offset, int length) {
        var scatterGatherElement = (ScatterGatherElement) Verbs.getPoolableInstance(ScatterGatherElement.class);
        scatterGatherElement.setAddress(data.getHandle() + offset);
        scatterGatherElement.setLength(length);
        scatterGatherElement.setLocalKey(data.getLocalKey());

        var receiveWorkRequest = new ReceiveWorkRequest.Builder().withScatterGatherElement(scatterGatherElement).build();

        queuePair.postReceive(receiveWorkRequest);

        return receiveWorkRequest.getId();
    }

    private CompletionQueue.WorkCompletionArray pollReceiveCompletions(int count) {
        var completionArray = new CompletionQueue.WorkCompletionArray(count);
        getReceiveCompletionQueue().poll(completionArray);


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
        getSendCompletionQueue().poll(completionArray);


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

    @Override
    public void close() throws IOException {
        queuePair.close();
        super.close();
    }

    public static ConnectionType getConnectionType() {
        return connectionType;
    }



}
