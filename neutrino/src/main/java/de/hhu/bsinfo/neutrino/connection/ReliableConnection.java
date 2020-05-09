package de.hhu.bsinfo.neutrino.connection;

import de.hhu.bsinfo.neutrino.buffer.RegisteredBuffer;
import de.hhu.bsinfo.neutrino.connection.interfaces.Connectable;
import de.hhu.bsinfo.neutrino.connection.message.Message;
import de.hhu.bsinfo.neutrino.connection.message.MessageType;
import de.hhu.bsinfo.neutrino.connection.util.ConcurrentRingBufferPool;
import de.hhu.bsinfo.neutrino.connection.util.RCInformation;
import de.hhu.bsinfo.neutrino.util.Poolable;
import de.hhu.bsinfo.neutrino.verbs.*;
import org.jctools.maps.NonBlockingHashMapLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.StringJoiner;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Implementation of a reliable connection (RC) type queue pair.
 *
 * @author Christian Gesse
 */
public class ReliableConnection extends QPSocket implements Connectable<RCInformation> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ReliableConnection.class);

    /**
     * Invalid local id - used to indicate invalid entries.
     */
    private static final short INVALID_LID = Short.MAX_VALUE;

    /**
     * Timeout in milliseconds for handshake during connection initialization phase
     */
    private static final long HANDSHAKE_CONNECT_TIMEOUT_MS = 2000;

    /**
     * Timeout in milliseconds for handshake during disconnect phase. Do not choose to small
     * since all pending work requests have to be completed during this phase
     */
    private static final long HANDSHAKE_DISCONNECT_TIMEOUT_MS = 10000;

    /**
     * Timeout in milliseconds for polling the handshake queue.
     */
    private static final int HANDSHAKE_POLL_QUEUE_TIMEOUT_MS = 40;

    /**
     * Standard pool size for work request map elements
     */
    private static final int WORK_REQUEST_MAP_ELEMENT_POOL_SIZE = 2048;

    /**
     * Atomic counter to provide global connection ids.
     */
    private static final AtomicLong connectionIdCounter = new AtomicLong(1);

    /**
     * Atomic counter to provide global work request ids.
     */
    protected static final AtomicLong wrIdProvider = new AtomicLong(1);

    /**
     * Efficient hash map containing work request ids and corresponding information for the work request.
     * These information are used when work completions are processed.
     */
    private static final NonBlockingHashMapLong<WorkRequestMapElement> workRequestMap = new NonBlockingHashMapLong<>();

    /**
     * Ring buffer pool to provide work request elements containing information about the work request.
     */
    private static final ConcurrentRingBufferPool<WorkRequestMapElement> workRequestMapElementPool = new ConcurrentRingBufferPool<WorkRequestMapElement>(WORK_REQUEST_MAP_ELEMENT_POOL_SIZE, WorkRequestMapElement::new);

    /**
     * Global id of the connection
     */
    private final long id;

    /**
     * Remote local id corresponding to the remote queue pair.
     */
    private AtomicInteger remoteLocalId = new AtomicInteger(INVALID_LID);

    /**
     * Atomic boolean indicating if this connection is connected yet.
     */
    private final AtomicBoolean isConnected = new AtomicBoolean(false);

    /**
     * Atomic boolean indicating if connection is connecting or disconnecting yet.
     */
    private final AtomicBoolean changeConnection = new AtomicBoolean(false);

    /**
     * Concurrent queue for infoamtion transport during handshakes.
     */
    private final RCHandshakeQueue handshakeQueue;

    /**
     * Fixed buffer to send handshake message
     */
    private final RegisteredBuffer handshakeSendBuffer;

    /**
     * Fixed buffer to receive handshake message
     */
    private final RegisteredBuffer handshakeReceiveBuffer;

    /**
     * Determines if handshake is performed when connectin to remote queue pair
     */
    private boolean doConnectHandshake = false;

    /**
     * Creates a new Reliable connection.
     *
     * @param deviceContext the device context
     * @throws IOException thrown if connection creation failes
     */
    public ReliableConnection(DeviceContext deviceContext) throws IOException {

        super(deviceContext);

        handshakeSendBuffer = deviceContext.allocRegisteredBuffer(Message.getSize());
        handshakeReceiveBuffer = deviceContext.allocRegisteredBuffer(Message.getSize());

        handshakeQueue = new RCHandshakeQueue();

        id = connectionIdCounter.getAndIncrement();
        LOGGER.info("Create reliable connection with id {}", id);

        queuePair = deviceContext.getProtectionDomain().createQueuePair(new QueuePair.InitialAttributes.Builder(
                QueuePair.Type.RC, sendCompletionQueue, receiveCompletionQueue, sendQueueSize, receiveQueueSize, 1, 1).build());
        if(queuePair == null) {
            throw new IOException("Cannot create queue pair");
        }
    }

    /**
     * Creates a new Reliable connection.
     *
     * @param deviceContext              the device context
     * @param sendQueueSize              the send queue size
     * @param receiveQueueSize           the receive queue size
     * @param sendCompletionQueueSize    the minimum send completion queue size
     * @param receiveCompletionQueueSize the minimum receive completion queue size
     * @throws IOException thrown if connection creation failes
     */
    public ReliableConnection(DeviceContext deviceContext, int sendQueueSize, int receiveQueueSize, int sendCompletionQueueSize, int receiveCompletionQueueSize) throws IOException {

        super(deviceContext,sendQueueSize, receiveQueueSize, sendCompletionQueueSize, receiveCompletionQueueSize);

        handshakeSendBuffer = deviceContext.allocRegisteredBuffer(Message.getSize());
        handshakeReceiveBuffer = deviceContext.allocRegisteredBuffer(Message.getSize());

        handshakeQueue = new RCHandshakeQueue();

        id = connectionIdCounter.getAndIncrement();
        LOGGER.info("Create reliable connection with id {}", id);

        queuePair = deviceContext.getProtectionDomain().createQueuePair(new QueuePair.InitialAttributes.Builder(
                QueuePair.Type.RC, sendCompletionQueue, receiveCompletionQueue, sendQueueSize, receiveQueueSize, 1, 1).build());
        if(queuePair == null) {
            throw new IOException("Cannot create queue pair");
        }
    }

    /**
     * Creates a new Reliable connection.
     *
     * @param deviceContext          the device context
     * @param sendQueueSize          the send queue size
     * @param receiveQueueSize       the receive queue size
     * @param sendCompletionQueue    the send completion queue
     * @param receiveCompletionQueue the receive completion queue
     * @throws IOException thrown if connection creation failes
     */
    public ReliableConnection(DeviceContext deviceContext, int sendQueueSize, int receiveQueueSize, CompletionQueue sendCompletionQueue, CompletionQueue receiveCompletionQueue) throws IOException {

        super(deviceContext, sendQueueSize, receiveQueueSize, sendCompletionQueue, receiveCompletionQueue);

        handshakeSendBuffer = deviceContext.allocRegisteredBuffer(Message.getSize());
        handshakeReceiveBuffer = deviceContext.allocRegisteredBuffer(Message.getSize());

        handshakeQueue = new RCHandshakeQueue();

        id = connectionIdCounter.getAndIncrement();
        LOGGER.info("Create reliable connection with id {}", id);

        queuePair = deviceContext.getProtectionDomain().createQueuePair(new QueuePair.InitialAttributes.Builder(
                QueuePair.Type.RC, sendCompletionQueue, receiveCompletionQueue, sendQueueSize, receiveQueueSize, 1, 1).build());
        if(queuePair == null) {
            throw new IOException("Cannot create queue pair");
        }
    }


    /**
     * Initializes the connection and transforms queue piar into init state.
     */
    @Override
    public void init() throws IOException {
        if(!queuePair.modify(QueuePair.Attributes.Builder.buildInitAttributesRC((short) 0, (byte) 1, AccessFlag.LOCAL_WRITE, AccessFlag.REMOTE_READ, AccessFlag.REMOTE_WRITE))) {
            throw new IOException("Unable to move queue pair into INIT state");
        }
    }

    /**
     * Resets queue pair and transforms it into reset state.
     *
     * @throws IOException the io exception
     */
    public void reset() throws IOException {
        if(!queuePair.modify(QueuePair.Attributes.Builder.buildResetAttributesRC())) {
            throw new IOException("Unable to move queue pair into RESET state");
        }
    }

    /**
     * Connects queue pair to remote and transforms it into ready to send state.
     *
     * @param the information about remote quuee pair
     * @return boolean indicating if connection was established successfully
     */
    @Override
    public boolean connect(RCInformation remoteInfo) throws IOException {

        // indicate that connection state is changing and stop if necessary
        if(changeConnection.getAndSet(true)){
            return isConnected.get();
        }

        // transform queue pair into ready to receive state
        if(!queuePair.modify(QueuePair.Attributes.Builder.buildReadyToReceiveAttributesRC(
                remoteInfo.getQueuePairNumber(), remoteInfo.getLocalId(), remoteInfo.getPortNumber()))) {
            LOGGER.error("Unable to move queue pair into RTR state");
            return false;
        }

        LOGGER.trace("Moved queue pair into RTR state with remote {}", remoteInfo.getLocalId());

        // transform queue pair into ready to send state
        if(!queuePair.modify(QueuePair.Attributes.Builder.buildReadyToSendAttributesRC())) {
            LOGGER.error("Unable to move queue pair into RTS state");
            return false;
        }

        LOGGER.trace("Moved queue pair into RTS state");

        // set remote local id
        remoteLocalId.set(remoteInfo.getLocalId());

        // try handshake with remote queue pair
        if(doConnectHandshake) {
            if(!handshake(MessageType.RC_CONNECT, HANDSHAKE_CONNECT_TIMEOUT_MS)) {
                // reset queue pair if handshake failed
                reset();
                init();

                changeConnection.set(false);
                return false;
            }
        }


        // change connection state
        isConnected.getAndSet(true);

        LOGGER.info("Connected RC with id {} to remote {}", id, remoteInfo.getLocalId());

        return true;
    }

    /**
     * Send data to remote queue pair.
     *
     * @param data the registered buffer conatining the data
     * @return the work request id
     * @throws IOException thrown if posting the request failes
     */
    public long send(RegisteredBuffer data) throws IOException  {
        return send(data, 0,  data.capacity());
    }

    /**
     * Send data to remote queue pair.
     *
     * @param data   the registered buffer conatining the data
     * @param offset the offset into the data buffer
     * @param length the amount of bytes to be send
     * @return the work request id
     * @throws IOException thrown if posting the request failes
     */
    public long send(RegisteredBuffer data, long offset, long length) throws IOException {
        if (isConnected.get()) {
             return internalSend(data, offset, length);
        } else {
            throw new IOException("Could not send data - RC is not connected yet.");
        }
    }

    /**
     * Internal method to post send work request. Can be used directly to post requests during
     * connecting/disconnecting phase.
     *
     * @param data   the registered buffer conatining the data
     * @param offset the offset into the data buffer
     * @param length the amount of bytes to be send
     * @return  the work request id
     */
    private long internalSend(RegisteredBuffer data, long offset, long length) {
        // get a new scatter gather element from pool and configure it
        var scatterGatherElement = (ScatterGatherElement) Verbs.getPoolableInstance(ScatterGatherElement.class);
        scatterGatherElement.setAddress(data.getHandle() + offset);
        scatterGatherElement.setLength((int) length);
        scatterGatherElement.setLocalKey(data.getLocalKey());

        // get a global id for the work request
        var wrId = wrIdProvider.getAndIncrement();

        // build the new send work request
        var sendWorkRequest = buildSendWorkRequest(scatterGatherElement, wrId);

        // put work request and corresponding information into map for later use
        workRequestMap.put(wrId, workRequestMapElementPool.getInstance().setRemoteLocalId((short) remoteLocalId.get()).setConnection(this).setSendWorkRequest(sendWorkRequest).setScatterGatherElement(scatterGatherElement));

        // post the send work reuqest
        postSend(sendWorkRequest);

        return wrId;
    }

    /**
     * Build send work request send work request.
     *
     * @param sge the scatter gather element containing information about the buffer
     * @param id  the global work request id
     * @return the send work request
     */
    protected SendWorkRequest buildSendWorkRequest(ScatterGatherElement sge, long id) {
        return new SendWorkRequest.MessageBuilder(SendWorkRequest.OpCode.SEND, sge).withSendFlags(SendWorkRequest.SendFlag.SIGNALED).withId(id).build();
    }

    /**
     * Post a  RDMA work request onto the send queue.
     *
     * @param data          the local registered buffer that should be used for the RDMA operation
     * @param opCode        the op code to be used (read or write)
     * @param offset        the offset into the data buffer
     * @param length        the length of the data to be read or written
     * @param remoteAddress the address of the remote buffer
     * @param remoteKey     the key of the remote buffer
     * @param remoteOffset  the offset into the remote buffer
     * @return the work request id
     * @throws IOException the io exception
     */
    public long execute(RegisteredBuffer data, SendWorkRequest.OpCode opCode, long offset, long length, long remoteAddress, int remoteKey, long remoteOffset) throws IOException  {
        if(isConnected.get()) {
            return internalExecute(data, opCode, offset, length, remoteAddress, remoteKey, remoteOffset);
        } else {
            throw new IOException("Could not execute on remote - RC is not connected yet.");
        }
    }

    /**
     * Internal method to execute RDMA operation.
     *
     * @param data          the local registered buffer that should be used for the RDMA operation
     * @param opCode        the op code to be used (read or write)
     * @param offset        the offset into the data buffer
     * @param length        the length of the data to be read or written
     * @param remoteAddress the address of the remote buffer
     * @param remoteKey     the key of the remote buffer
     * @param remoteOffset  the offset into the remote buffer
     * @return the work request id
     */
    private long internalExecute(RegisteredBuffer data, SendWorkRequest.OpCode opCode, long offset, long length, long remoteAddress, int remoteKey, long remoteOffset) {
        // get a new scatter gather element from pool and configure it
        var scatterGatherElement = (ScatterGatherElement) Verbs.getPoolableInstance(ScatterGatherElement.class);
        scatterGatherElement.setAddress(data.getHandle() + offset);
        scatterGatherElement.setLength((int) length);
        scatterGatherElement.setLocalKey(data.getLocalKey());

        // get a global id for the work request
        var wrId = wrIdProvider.getAndIncrement();
        // build the new send work request for the RDMA operation
        var sendWorkRequest = buildRDMAWorkRequest(opCode, scatterGatherElement, remoteAddress + remoteOffset, remoteKey, wrId);
        // put work request and corresponding information into map for later use
        workRequestMap.put(wrId, workRequestMapElementPool.getInstance().setRemoteLocalId((short) remoteLocalId.get()).setConnection(this).setSendWorkRequest(sendWorkRequest).setScatterGatherElement(scatterGatherElement));

        // post work request onto send queue
        postSend(sendWorkRequest);

        return wrId;
    }

    /**
     * Build work request for RDMA operation.
     *
     * @param opCode        the op code of the RDMA operation
     * @param sge           the scatter gather element containing information about the local buffer
     * @param remoteAddress the remote memory address
     * @param remoteKey     the key of the remote buffer
     * @param id            the work request id
     * @return the send work request
     */
    protected SendWorkRequest buildRDMAWorkRequest(SendWorkRequest.OpCode opCode, ScatterGatherElement sge, long remoteAddress, int remoteKey, long id) {
        return new SendWorkRequest.RdmaBuilder(opCode, sge, remoteAddress, remoteKey).withSendFlags(SendWorkRequest.SendFlag.SIGNALED).withId(id).build();
    }

    /**
     * Post receive work request.
     *
     * @param data the local buffer that should contain the received data
     * @return the work request id
     * @throws IOException thrown if posting the request failes
     */
    public long receive(RegisteredBuffer data) throws IOException {
        return receive(data, 0, data.capacity());
    }

    /**
     * Post receive work request.
     *
     * @param data the local buffer that should contain the received data
     * @param offset the offset into the buffer
     * @param length the length of the data
     * @return the work request id
     * @throws IOException thrown if posting the request failes
     */
    public long receive(RegisteredBuffer data, long offset, long length) throws IOException {
        if(isConnected.get()) {
            return internalReceive(data, offset, length);
        } else {
            throw new IOException("Could not receive - RC is not connected yet.");
        }
    }

    /**
     * Internal method to post receive work request. Can be used directly to post requests during
     * connecting/disconnecting phase.
     *
     * @param data the local buffer that should contain the received data
     * @param offset the offset into the buffer
     * @param length the length of the data
     * @return the work request id
     */
    private long internalReceive(RegisteredBuffer data, long offset, long length) {
        // get a new scatter gather element from pool and configure it
        var scatterGatherElement = (ScatterGatherElement) Verbs.getPoolableInstance(ScatterGatherElement.class);
        scatterGatherElement.setAddress(data.getHandle() + offset);
        scatterGatherElement.setLength((int) length);
        scatterGatherElement.setLocalKey(data.getLocalKey());

        // get a global id for the work request
        var wrId = wrIdProvider.getAndIncrement();
        // build the new send work request for the receive operation
        var receiveWorkRequest = buildReceiveWorkRequest(scatterGatherElement, wrId);
        // put work request and corresponding information into map for later use

        workRequestMap.put(wrId, workRequestMapElementPool.getInstance().setRemoteLocalId((short) remoteLocalId.get()).setConnection(this).setReceiveWorkRequest(receiveWorkRequest).setScatterGatherElement(scatterGatherElement));

        // post receive work request onto queue
        postReceive(receiveWorkRequest);

        return wrId;
    }

    /**
     * Build receive work request receive work request.
     *
     * @param sge the scatter gather element conatining information abut the buffer
     * @param id  the work request id
     * @return the receive work request
     */
    protected ReceiveWorkRequest buildReceiveWorkRequest(ScatterGatherElement sge, long id) {
        return new ReceiveWorkRequest.Builder().withScatterGatherElement(sge).withId(id).build();
    }

    /**
     * Reset queue pair if error occured and transform into init state.
     *
     * @throws IOException thrown if initialiaztion failes
     */
    public void resetFromError() throws IOException {
        if(queuePair.getState() == QueuePair.State.ERR) {
            reset();
            init();
        }
    }

    /**
     * Performs a handshake with the remote queue pair. Can be used to make sure that remote queue pair
     * is ready when connecting or to make sure that all pending work requests are processes on bot sides
     * when disconnecting. Changing the queue pairs without handshake does not guarantee this.
     *
     * @param msgType   the message type to be used for handshake (connect/disconnect)
     * @param timeOutMs   the timeout for the handshake in milliseconds
     * @return
     * @throws IOException
     */
    private boolean handshake(MessageType msgType, long timeOutMs) throws IOException {
        LOGGER.trace("Handshake of connection {} with message {} started", getId(), msgType);

        // convert timeout into nanoseconds
        var timeOutNs = TimeUnit.NANOSECONDS.convert(timeOutMs, TimeUnit.MILLISECONDS);

        // create new message for the handshake
        var message = new Message(handshakeSendBuffer);
        message.setMessageType(msgType);
        message.setId(Message.provideGlobalId());
        // prepare buffer to receive message from remote queue pair
        var receiveBuffer = handshakeReceiveBuffer;
        receiveBuffer.clear();

        // post recieve work request for remote message
        var receiveWrId = internalReceive(receiveBuffer, 0, receiveBuffer.capacity());

        // boolean indicating timeout was reached
        boolean isTimeOut = false;

        // boolean indicating the send qork request was sent successfully
        boolean isSent = false;
        // boolean indiciating that message from remote was received successfully
        boolean isReceived = false;

        var startTime = System.nanoTime();

        // send handshake message
        internalSend(message.getByteBuffer(), 0, message.getNativeSize());

        // wait until message was sent and received or timout occured
        while(!(isSent && isReceived) && !isTimeOut) {

            // check if timeout occured
            if(System.nanoTime() - startTime > timeOutNs) {
                isTimeOut = true;
            } else {
                try {
                    // poll handshake queue to receive information about work request state
                    var value = handshakeQueue.poll(HANDSHAKE_POLL_QUEUE_TIMEOUT_MS, TimeUnit.MILLISECONDS);

                    // sending the message was successful
                    if(value == RCHandshakeQueue.SEND_COMPLETE) {
                        isSent = true;
                    // remote message recieved
                    } else if(value == RCHandshakeQueue.RECEIVE_COMPLETE) {
                        isReceived = true;
                    // error occured sending the message
                    } else if(value == RCHandshakeQueue.SEND_ERROR) {
                        LOGGER.error("Error sending handshake message");
                    }
                } catch (Exception e) {
                    LOGGER.trace("Exception accured during polling: {}", e);
                }
            }
        }

        if(!isTimeOut) {
            LOGGER.info("Handshake of connection {}  with message {} finished", getId(), msgType);
        }

        // if timeout did not occur, the handshake was successful
        return !isTimeOut;
    }

    /**
     * Disconnects from remote queue pair and transforms into init state.
     **/
    @Override
    public boolean disconnect() throws IOException{

        // either another thread is alreading disconnecting this connection or it is in unconnected state
        if(!isConnected.getAndSet(false)) {
            throw new IOException("Connection already disconnecting or disconnected");
        }

        LOGGER.trace("Start to disconnect connection {} from {}", id, remoteLocalId);

        // handshake to make shure that all pending requests are processed
        if(handshake(MessageType.RC_DISCONNECT, HANDSHAKE_DISCONNECT_TIMEOUT_MS)) {
            // set remote local id
            remoteLocalId.getAndSet(INVALID_LID);

            // init and reset queue pair
            reset();
            init();

            // now connection is ready to be connected
            changeConnection.getAndSet(false);
            LOGGER.info("Disconnected connection {}", id);

            return true;
        } else {
            // if handshake fails, connection should not be disconnected
            LOGGER.info("Did not disconnect connection {}", id);

            return false;
        }
    }


    /**
     * Closes the queue pair.
     */
    @Override
    public void close() throws IOException {
        reset();
        LOGGER.info("Close reliable connection {}", id);
        queuePair.close();
    }

    /**
     * Returns the id f the quuee pair.
     *
     * @return the connecion id
     */
    public long getId() {
        return id;
    }

    public QueuePair getQueuePair() {
        return queuePair;
    }

    /**
     * Returns if queue pair is connected.
     *
     * @return boolean if connection is connected
     **/
    public boolean isConnected() {
        return isConnected.get();
    }

    /**
     * Returns remote local id.
     *
     * @return the remote local id or the INVALID_LID if not connected
     */
    public short getRemoteLocalId() {
        return (short) remoteLocalId.get();
    }

    /**
     * Sets the conenct handshake indicator.
     *
     * @param val value to set
     */
    public void setDoConnectHandshake(boolean val) {
        doConnectHandshake = val;
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

    /**
     * Returns the handshake queue.
     *
     * @return the handshake queue
     */
    public RCHandshakeQueue getHandshakeQueue() {
        return handshakeQueue;
    }

    /**
     * Fetch work request data from map.
     *
     * @param wrId the work request id
     * @return the work request map element
     */
    public static WorkRequestMapElement fetchWorkRequestDataData(long wrId) {
        return workRequestMap.remove(wrId);
    }


    /**
     * Handshake queue to hold information from work completions during handshake
     */
    public class RCHandshakeQueue  {
        /**
         * The constant SEND_COMPLETE - indicates that handshake message ws sent successfully
         */
        protected static final int SEND_COMPLETE = 0;

        /**
         * The constant RECEIVE_COMPLETE - indicates that message from remote was received
         */
        protected static final int RECEIVE_COMPLETE = 1;

        /**
         * The constant SEND_ERROR - indicates that sending the message resulted in an error
         */
        protected static final int SEND_ERROR = 2;

        /**
         * The concurret queue.
         */
        protected final ArrayBlockingQueue<Integer> queue;

        /**
         * Capacity of the queue.
         */
        private static final int CAPACITY = 5;

        /**
         * Instantiates a new handshake queue.
         */
        public RCHandshakeQueue() {
            this.queue = new ArrayBlockingQueue<>(CAPACITY);
        }

        /**
         * Push send complete event to queue.
         */
        public void pushSendComplete() {
            queue.add(SEND_COMPLETE);
        }

        /**
         * Push receive evet to queue.
         */
        public void pushReceiveComplete() {
            queue.add(RECEIVE_COMPLETE);
        }

        /**
         * Push send error event to queue.
         */
        public void pushSendError() {
            queue.add(SEND_ERROR);
        }

        /**
         * Poll next element from queue until timeout is reached.
         *
         * @param timeout the timeout
         * @param unit    the unit of the timeout
         * @return the event from the queue
         * @throws InterruptedException the interrupted exception
         */
        public int poll(int timeout, TimeUnit unit) throws InterruptedException {
            return queue.poll(timeout, unit);
        }

        /**
         * Poll next element from queue.
         *
         * @return theevent from th queue.
         */
        public int poll() {
            return queue.poll();
        }

    }

    /**
     * An element containing all necessary informaton about a work request.
     * Is used to save these information until the work completion is polled.
     * Is poolable to be able to get new instances in an efficient way.
     */
    public static class WorkRequestMapElement implements Poolable {

        /**
         * The send work request.
         */
        public SendWorkRequest sendWorkRequest;

        /**
         * The seceive work request.
         */
        public ReceiveWorkRequest receiveWorkRequest;

        /**
         * The scatter gather element.
         */
        public ScatterGatherElement scatterGatherElement;

        /**
         * The connection.
         */
        public ReliableConnection connection;

        /**
         * The remote local id.
         */
        public short remoteLocalId;

        /**
         * Instantiates a new Work request map element.
         */
        WorkRequestMapElement() {}

        /**
         * Sets receive work request.
         *
         * @param receiveWorkRequest the receive work request
         * @return the work request map element
         */
        public WorkRequestMapElement setReceiveWorkRequest(ReceiveWorkRequest receiveWorkRequest) {
            this.receiveWorkRequest = receiveWorkRequest;

            return this;
        }

        /**
         * Sets send work request.
         *
         * @param sendWorkRequest the send work request
         * @return the work request map element
         */
        public WorkRequestMapElement setSendWorkRequest(SendWorkRequest sendWorkRequest) {
            this.sendWorkRequest = sendWorkRequest;

            return this;
        }

        /**
         * Sets remote local id.
         *
         * @param remoteLocalId the remote local id
         * @return the work request map element
         */
        public WorkRequestMapElement setRemoteLocalId(short remoteLocalId) {
            this.remoteLocalId = remoteLocalId;

            return this;
        }

        /**
         * Sets scatter gather element.
         *
         * @param scatterGatherElement the scatter gather element
         * @return the work request map element
         */
        public WorkRequestMapElement setScatterGatherElement(ScatterGatherElement scatterGatherElement) {
            this.scatterGatherElement = scatterGatherElement;

            return this;
        }

        /**
         * Sets connection.
         *
         * @param connection the connection
         * @return the work request map element
         */
        public WorkRequestMapElement setConnection(ReliableConnection connection) {
            this.connection = connection;

            return this;
        }

        @Override
        /**
         * Releases the instance into the pool for later use.
         * Resets all parameters.
         */
        public void releaseInstance() {
            sendWorkRequest = null;
            receiveWorkRequest = null;
            scatterGatherElement = null;
            connection = null;
            remoteLocalId = 0;

            workRequestMapElementPool.returnInstance(this);
        }
    }
}
