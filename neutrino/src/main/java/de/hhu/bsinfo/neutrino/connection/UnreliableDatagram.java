package de.hhu.bsinfo.neutrino.connection;

import de.hhu.bsinfo.neutrino.buffer.RegisteredBuffer;
import de.hhu.bsinfo.neutrino.connection.util.ConnectionInformation;
import de.hhu.bsinfo.neutrino.connection.util.UDRemoteInformation;
import de.hhu.bsinfo.neutrino.verbs.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class UnreliableDatagram extends QPSocket{
    private static final Logger LOGGER = LoggerFactory.getLogger(UnreliableDatagram.class);

    // Offset in Received Buffers (first 40 Bytes are used for MetaInfo)
    public static final int UD_Receive_Offset = 40;

    public UnreliableDatagram(DeviceContext deviceContext) throws IOException {

        super(deviceContext);

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

    public long send(RegisteredBuffer data, UDRemoteInformation remoteInfo) {
        return send(data, 0, data.capacity(), remoteInfo);
    }

    public long send(RegisteredBuffer data, long offset, long length, UDRemoteInformation remoteInfo) {
        var addressAttributes = new AddressHandle.Attributes.Builder(remoteInfo.getLocalId(), remoteInfo.getPortNumber()).build();
        var addressHandle = getDeviceContext().getProtectionDomain().createAddressHandle(addressAttributes);

        var scatterGatherElement = (ScatterGatherElement) Verbs.getPoolableInstance(ScatterGatherElement.class);
        scatterGatherElement.setAddress(data.getHandle() + offset);
        scatterGatherElement.setLength((int) length);
        scatterGatherElement.setLocalKey(data.getLocalKey());

        var sendWorkRequest = new SendWorkRequest.UnreliableBuilder(SendWorkRequest.OpCode.SEND, scatterGatherElement, addressHandle, remoteInfo.getQueuePairNumber(), remoteInfo.getQueuePairKey()).withId(wrIdProvider.getAndIncrement()).withSendFlags(SendWorkRequest.SendFlag.SIGNALED).build();

        return postSend(sendWorkRequest);
    }

    public long receive(RegisteredBuffer data) {
        return receive(data, 0, data.capacity());
    }

    public long receive(RegisteredBuffer data, long offset, long length) {
        var scatterGatherElement = (ScatterGatherElement) Verbs.getPoolableInstance(ScatterGatherElement.class);
        scatterGatherElement.setAddress(data.getHandle() + offset);
        scatterGatherElement.setLength((int) length);
        scatterGatherElement.setLocalKey(data.getLocalKey());

        var receiveWorkRequest = new ReceiveWorkRequest.Builder().withScatterGatherElement(scatterGatherElement).withId(wrIdProvider.getAndIncrement()).build();

        return postReceive(receiveWorkRequest);
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

    @Override
    public void close() {
        queuePair.close();
    }
}
