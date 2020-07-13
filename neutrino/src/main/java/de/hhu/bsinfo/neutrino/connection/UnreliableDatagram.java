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


/**
 * Implementation of a unreliable datagram (UD) type queue pair.
 *
 * @author Christian Gesse
 */
public class UnreliableDatagram extends QPSocket{
    private static final Logger LOGGER = LoggerFactory.getLogger(UnreliableDatagram.class);

    /**
     * Global counter for UD ids
     */
    private static final AtomicInteger idCounter = new AtomicInteger(0);
    /**
     * Id of this UD queue pair
     */
    private final int id;

    /**
     * The global id provider for work requests und UD queue pairs
     */
    protected static final AtomicLong wrIdProvider = new AtomicLong(0);

    /**
     * Constant offset into buffers of received datagrams (first 40 bytes are used for meta information)
     */
    public static final int UD_Receive_Offset = 40;

    /**
     * Instantiates a new Unreliable datagram.
     *
     * @param deviceContext the device context
     * @throws IOException the io exception
     */
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

    /**
     * Instantiates a new Unreliable datagram.
     *
     * @param deviceContext              the device context
     * @param sendQueueSize              the send queue size
     * @param receiveQueueSize           the receive queue size
     * @param sendCompletionQueueSize    the send completion queue size
     * @param receiveCompletionQueueSize the receive completion queue size
     * @throws IOException the io exception
     */
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

    /**
     * Instantiates a new Unreliable datagram.
     *
     * @param deviceContext          the device context
     * @param sendQueueSize          the send queue size
     * @param receiveQueueSize       the receive queue size
     * @param sendCompletionQueue    the send completion queue
     * @param receiveCompletionQueue the receive completion queue
     * @throws IOException the io exception
     */
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

    /**
     * Initializes the queue pair as UD and sets it into RTS state.
     *
     * @throws IOException exception if error occurs
     */
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

    /**
     * Posts send work request onto queue pair.
     *
     * @param data       the registered buffer containing the data
     * @param remoteInfo the information about the remote queue pair
     * @return id of the work request
     */
    public long send(RegisteredBuffer data, UDInformation remoteInfo) {
        return send(data, 0, data.capacity(), remoteInfo);
    }

    /**
     * Posts send work request onto queue pair.
     *
     * @param data       the registered buffer containing the data
     * @param offset     the offset into the buffer
     * @param length     the length of the data
     * @param remoteInfo the information about the remote queue pair
     * @return id of the work request
     */
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

    /**
     * Build a send work request.
     *
     * @param sge        the scatter gather element
     * @param remoteInfo the information about the remote queue pair
     * @param wrId       the work request id
     * @return the send work request
     */
    protected SendWorkRequest buildSendWorkRequest(ScatterGatherElement sge, UDInformation remoteInfo, long wrId) {
        var addressAttributes = new AddressHandle.Attributes.Builder(remoteInfo.getLocalId(), remoteInfo.getPortNumber()).build();
        var addressHandle = getDeviceContext().getProtectionDomain().createAddressHandle(addressAttributes);

        return new SendWorkRequest.UnreliableBuilder(SendWorkRequest.OpCode.SEND, sge, addressHandle, remoteInfo.getQueuePairNumber(), remoteInfo.getQueuePairKey()).withId(wrId).withSendFlags(SendWorkRequest.SendFlag.SIGNALED).build();
    }

    /**
     * Posts receive work request onto queue pair.
     *
     * @param data       the registered buffer for the received data
     * @return id of the work request
     */
    public long receive(RegisteredBuffer data) {
        return receive(data, 0, data.capacity());
    }

    /**
     * Posts receive work request onto queue pair.
     *
     * @param data       the registered buffer for the received data
     * @param offset     the offset into the buffer
     * @param length     the length of the data
     * @return id of the work request
     */
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

    /**
     * Build a receive work request.
     *
     * @param sge        the scatter gather element
     * @param wrId       the work request id
     * @return the send work request
     */
    protected ReceiveWorkRequest buildReceiveWorkRequest(ScatterGatherElement sge, long wrId) {
        return new ReceiveWorkRequest.Builder().withScatterGatherElement(sge).withId(wrId).build();
    }

    /**
     * Gets the id of this UD queue pair.
     *
     * @return the id
     */
    public int getId() {
        return id;
    }

    /**
     * Close this queue pair.
     */
    @Override
    public void close() {
        LOGGER.info("Close unreliable datagram {}", id);
        queuePair.close();
    }

    /**
     * Print out information about this queue pair as a string.
     *
     * @return string containing the information
     */
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
