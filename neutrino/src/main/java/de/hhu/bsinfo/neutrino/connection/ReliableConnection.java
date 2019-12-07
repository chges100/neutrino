package de.hhu.bsinfo.neutrino.connection;

import de.hhu.bsinfo.neutrino.buffer.RegisteredBuffer;
import de.hhu.bsinfo.neutrino.verbs.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;

public final class ReliableConnection extends Connection{

    private static final Logger LOGGER = LoggerFactory.getLogger(ReliableConnection.class);

    private final QueuePair queuePair;

    public ReliableConnection(int deviceNumber) throws IOException {

        super(deviceNumber);

        queuePair = getProtectionDomain().createQueuePair(new QueuePair.InitialAttributes.Builder(
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

    @Override
    public void send(RegisteredBuffer data) throws IOException {

    }

    @Override
    public void receive() throws IOException {

    }

    @Override
    public void close() throws IOException {
        queuePair.close();
        super.close();
    }


}
