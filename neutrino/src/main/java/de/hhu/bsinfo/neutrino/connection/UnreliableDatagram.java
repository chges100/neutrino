package de.hhu.bsinfo.neutrino.connection;

import de.hhu.bsinfo.neutrino.buffer.RegisteredBuffer;
import de.hhu.bsinfo.neutrino.connection.util.UDInformation;
import de.hhu.bsinfo.neutrino.verbs.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.StringJoiner;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class UnreliableDatagram extends QPSocket{
    private static final Logger LOGGER = LoggerFactory.getLogger(UnreliableDatagram.class);

    private static final AtomicInteger idCounter = new AtomicInteger(0);
    private final int id;

    protected static final AtomicLong wrIdProvider = new AtomicLong(0);

    // Offset in Received Buffers (first 40 Bytes are used for MetaInfo)
    public static final int UD_Receive_Offset = 40;

    public UnreliableDatagram(DeviceContext deviceContext) throws IOException {

        super(deviceContext);

        id = idCounter.getAndIncrement();

        LOGGER.info("Create new unreliable datagram with id {}", id);

        queuePair = deviceContext.getProtectionDomain().createQueuePair(new QueuePair.InitialAttributes.Builder(
                QueuePair.Type.UD, sendCompletionQueue, receiveCompletionQueue, sendQueueSize, receiveQueueSize, 1, 1).build());
        if(queuePair == null) {
            throw new IOException("Cannot create queue pair");
        }
    }

    public UnreliableDatagram(DeviceContext deviceContext, int sendQueueSize, int receiveQueueSize, int sendCompletionQueueSize, int receiveCompletionQueueSize) throws IOException {

        super(deviceContext, sendQueueSize, receiveQueueSize, sendCompletionQueueSize, receiveCompletionQueueSize);

        id = idCounter.getAndIncrement();

        LOGGER.info("Create new unreliable datagram with id {}", id);

        queuePair = deviceContext.getProtectionDomain().createQueuePair(new QueuePair.InitialAttributes.Builder(
                QueuePair.Type.UD, sendCompletionQueue, receiveCompletionQueue, sendQueueSize, receiveQueueSize, 1, 1).build());
        if(queuePair == null) {
            throw new IOException("Cannot create queue pair");
        }
    }

    public UnreliableDatagram(DeviceContext deviceContext, int sendQueueSize, int receiveQueueSize, CompletionQueue sendCompletionQueue, CompletionQueue receiveCompletionQueue) throws IOException {

        super(deviceContext, sendQueueSize, receiveQueueSize, sendCompletionQueue, receiveCompletionQueue);

        id = idCounter.getAndIncrement();

        LOGGER.info("Create new unreliable datagram with id {}", id);

        queuePair = deviceContext.getProtectionDomain().createQueuePair(new QueuePair.InitialAttributes.Builder(
                QueuePair.Type.UD, sendCompletionQueue, receiveCompletionQueue, sendQueueSize, receiveQueueSize, 1, 1).build());
        if(queuePair == null) {
            throw new IOException("Cannot create queue pair");
        }
    }

    @Override
    public void init() throws IOException {
        if(!queuePair.modify(QueuePair.Attributes.Builder.buildInitAttributesUD((short) 0, (byte) 1))) {
            throw new IOException("Unable to move queue pair into INIT state");
        }


        if(!queuePair.modify(QueuePair.Attributes.Builder.buildReadyToReceiveAttributesUD())) {
            throw new IOException("Unable to move queue pair into RTR state");
        }


        if(!queuePair.modify(QueuePair.Attributes.Builder.buildReadyToSendAttributesUD())) {
            throw new IOException("Unable to move queue pair into RTS state");
        }
    }

    public long send(RegisteredBuffer data, UDInformation remoteInfo) {
        return send(data, 0, data.capacity(), remoteInfo);
    }

    public long send(RegisteredBuffer data, long offset, long length, UDInformation remoteInfo) {


        var scatterGatherElement = (ScatterGatherElement) Verbs.getPoolableInstance(ScatterGatherElement.class);
        scatterGatherElement.setAddress(data.getHandle() + offset);
        scatterGatherElement.setLength((int) length);
        scatterGatherElement.setLocalKey(data.getLocalKey());

        var wrId = wrIdProvider.getAndIncrement();

        var sendWorkRequest = buildSendWorkRequest(scatterGatherElement, remoteInfo, wrId);

        postSend(sendWorkRequest);

        sendWorkRequest.releaseInstance();
        scatterGatherElement.releaseInstance();

        return wrId;
    }

    protected SendWorkRequest buildSendWorkRequest(ScatterGatherElement sge, UDInformation remoteInfo, long wrId) {
        var addressAttributes = new AddressHandle.Attributes.Builder(remoteInfo.getLocalId(), remoteInfo.getPortNumber()).build();
        var addressHandle = getDeviceContext().getProtectionDomain().createAddressHandle(addressAttributes);

        return new SendWorkRequest.UnreliableBuilder(SendWorkRequest.OpCode.SEND, sge, addressHandle, remoteInfo.getQueuePairNumber(), remoteInfo.getQueuePairKey()).withId(wrId).withSendFlags(SendWorkRequest.SendFlag.SIGNALED).build();
    }

    public long receive(RegisteredBuffer data) {
        return receive(data, 0, data.capacity());
    }

    public long receive(RegisteredBuffer data, long offset, long length) {
        var scatterGatherElement = (ScatterGatherElement) Verbs.getPoolableInstance(ScatterGatherElement.class);
        scatterGatherElement.setAddress(data.getHandle() + offset);
        scatterGatherElement.setLength((int) length);
        scatterGatherElement.setLocalKey(data.getLocalKey());

        var wrId = wrIdProvider.getAndIncrement();

        var receiveWorkRequest = buildReceiveWorkRequest(scatterGatherElement, wrId);

        postReceive(receiveWorkRequest);

        receiveWorkRequest.releaseInstance();
        scatterGatherElement.releaseInstance();

        return wrId;
    }

    protected ReceiveWorkRequest buildReceiveWorkRequest(ScatterGatherElement sge, long wrId) {
        return new ReceiveWorkRequest.Builder().withScatterGatherElement(sge).withId(wrId).build();
    }

    public int getId() {
        return id;
    }

    @Override
    public void close() {
        LOGGER.info("Close unreliable datagram {}", id);
        queuePair.close();
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", UnreliableDatagram.class.getSimpleName() + "[", "]")
                .add("UD id=" + id)
                .add("localId=" + portAttributes.getLocalId())
                .add("portNumber=" + 1)
                .add("queuePairNumber=" + queuePair.getQueuePairNumber())
                .add("queuePairKey=" + queuePair.queryAttributes().getQkey())
                .toString();
    }
}
