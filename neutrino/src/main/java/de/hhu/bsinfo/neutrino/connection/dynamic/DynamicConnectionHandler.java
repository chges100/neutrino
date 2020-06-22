package de.hhu.bsinfo.neutrino.connection.dynamic;

import de.hhu.bsinfo.neutrino.buffer.BufferInformation;
import de.hhu.bsinfo.neutrino.buffer.LocalBuffer;
import de.hhu.bsinfo.neutrino.connection.DeviceContext;
import de.hhu.bsinfo.neutrino.connection.UnreliableDatagram;
import de.hhu.bsinfo.neutrino.connection.message.LocalMessage;
import de.hhu.bsinfo.neutrino.connection.message.Message;
import de.hhu.bsinfo.neutrino.connection.message.MessageType;
import de.hhu.bsinfo.neutrino.connection.util.RCInformation;
import de.hhu.bsinfo.neutrino.connection.util.SGEProvider;
import de.hhu.bsinfo.neutrino.connection.util.UDInformation;
import de.hhu.bsinfo.neutrino.util.NativeObjectRegistry;
import de.hhu.bsinfo.neutrino.verbs.*;
import org.jctools.maps.NonBlockingHashMapLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The dynamic connection handler.
 * Based on unreliable datagram to handle connection between nodes. Uses a thread poll to handle requests asynchronously.
 *
 * @author Christian Gesse
 */
public final class DynamicConnectionHandler extends UnreliableDatagram {
    private static final Logger LOGGER = LoggerFactory.getLogger(DynamicConnectionHandler.class);

    /**
     * The maximum of send work requests that is provided by the sge provider
     */
    private static final int MAX_SEND_WORK_REQUESTS = 200;
    /**
     * The maximum of receive work requests that is provided by the sge provider
     */
    private static final int MAX_RECEIVE_WORK_REQUESTS = 200;

    /**
     * Size of the send completion queue
     */
    private static final int SEND_COMPLETION_QUEUE_SIZE = 250;
    /**
     * Size of the receive completion queue
     */
    private static final int RECEIVE_COMPLETION_QUEUE_SIZE = 250;

    /**
     * Timeout before new connection request is sent if no response
     */
    private static final long RESP_CONNECTION_REQ_TIMEOUT_MS = 400;
    /**
     * Timeout before new buffer information is sent if no response
     */
    private static final long RESP_BUFFER_ACK_TIMEOUT_MS = 400;

    /**
     * Instance of the dynamic connection manager
     */
    private final DynamicConnectionManager dcm;
    /**
     * The table of remote connection handlers mapping the local id to the information
     */
    private final NonBlockingHashMapLong<UDInformation> remoteHandlerTable = new NonBlockingHashMapLong<>();
    /**
     * Table of handlers that are already registered - provides fast way to check if a remote node was already detected
     */
    private final RegisteredHandlerTable registeredHandlerTable;

    /**
     * The ud information about this node
     */
    private final UDInformation localUDInformation;

    /**
     * Array of posted send work requests
     */
    private final SendWorkRequest[] sendWorkRequests = new SendWorkRequest[MAX_SEND_WORK_REQUESTS];
    /**
     * Array of posted receive work requests
     */
    private final ReceiveWorkRequest[] receiveWorkRequests = new ReceiveWorkRequest[MAX_RECEIVE_WORK_REQUESTS];

    /**
     * Provider for send work request ids (the connection handler does not use the global id system for wrs)
     */
    private final AtomicInteger ownSendWrIdProvider = new AtomicInteger(0);
    /**
     * Provider for receive work request ids (the connection handler does not use the global id system for wrs)
     */
    private final AtomicInteger ownReceiveWrIdProvider = new AtomicInteger(0);

    /**
     * Map that holds status information about remote nodes (for example if connection request was accepted by remote node)
     */
    private final NonBlockingHashMapLong<RemoteStatus> remoteStatusMap = new NonBlockingHashMapLong<>();

    /**
     * Provider for scatter gather elements for (messaging) send word requests
     */
    private final SGEProvider sendSGEProvider;
    /**
     * Provider for scatter gather elements for (messaging) send word requests
     */
    private final SGEProvider receiveSGEProvider;

    /**
     * Thread for completion queue polling and work completion processing
     */
    private final UDCompletionQueuePollThread udCompletionPoller;

    /**
     * Instantiates a new Dynamic connection handler.
     *
     * @param dcm           the instance of the dynamic connection management
     * @param deviceContext the device context
     * @param maxLid        the maximum of valid local ids
     * @throws IOException the io exception
     */
    protected DynamicConnectionHandler(DynamicConnectionManager dcm, DeviceContext deviceContext, int maxLid) throws IOException {
        // create ud queue pair
        super(deviceContext, MAX_SEND_WORK_REQUESTS, MAX_RECEIVE_WORK_REQUESTS, SEND_COMPLETION_QUEUE_SIZE, RECEIVE_COMPLETION_QUEUE_SIZE);

        this.dcm = dcm;

        LOGGER.info("Set up dynamic connection handler");

        // create registered handler table with fixed size
        registeredHandlerTable = new RegisteredHandlerTable(maxLid);

        // create fast providers for scatter gather elements
        sendSGEProvider = new SGEProvider(getDeviceContext(), 2 * MAX_SEND_WORK_REQUESTS, Message.getSize());
        receiveSGEProvider = new SGEProvider(getDeviceContext(), 2 * MAX_RECEIVE_WORK_REQUESTS, Message.getSize() + UD_Receive_Offset);

        // initialize ud queue pair
        init();

        // create information about this handler
        localUDInformation = new UDInformation((byte) 1, getPortAttributes().getLocalId(), getQueuePair().getQueuePairNumber(),
                getQueuePair().queryAttributes().getQkey());

        // start polling of completion queue
        udCompletionPoller = new UDCompletionQueuePollThread();
        udCompletionPoller.start();
    }

    /**
     * Gets information about this handler.
     *
     * @return the ud information about this handler
     */
    protected UDInformation getLocalUDInformation() {
        return localUDInformation;
    }

    /**
     * Register remote connection handler.
     *
     * @param remoteLocalId     the local id of the remote node
     * @param remoteHandlerInfo the information about the remote handler
     */
    protected void registerRemoteConnectionHandler(int remoteLocalId, UDInformation remoteHandlerInfo) {
        remoteHandlerTable.put(remoteLocalId, remoteHandlerInfo);
        // mark handler as registered for fast checking
        registeredHandlerTable.setRegistered(remoteLocalId);
    }

    /**
     * Gets information about a remote handler
     *
     * @param remoteLocalId the local id of the remote node
     * @return the information about the remote handler
     */
    protected UDInformation getRemoteHandlerInfo(int remoteLocalId) {
        return remoteHandlerTable.get(remoteLocalId);
    }

    /**
     * Checks if information about a remote handler is available
     *
     * @param remoteLocalId the local id of the remote
     * @return the boolean indicating if remote handler info is available
     */
    protected boolean hasRemoteHandlerInfo(int remoteLocalId) {
        return remoteHandlerTable.containsKey(remoteLocalId);
    }

    /**
     * Get array of local ids of registered handlers.
     *
     * @return the array with remote local ids
     */
    protected short[] getRemoteLocalIds() {
        var n = remoteHandlerTable.size();
        var remotes = remoteHandlerTable.keySet().toArray();
        short[] lids = new short[n];

        for(int i = 0; i < lids.length; i++) {
            lids[i] =  (short) (long) remotes[i];
        }

        return lids;
    }

    /**
     * Initialize a connection request to a remote node.
     *
     * @param localQP       the information about the local rc queue pair of the connection
     * @param remoteLocalId the local id of the remote node
     */
    protected void initConnectionRequest(RCInformation localQP, short remoteLocalId) {
        // create remote status if necessary
        var remoteStatus = remoteStatusMap.putIfAbsent(remoteLocalId, new RemoteStatus());

        // poll until remote UD pair data is available
        registeredHandlerTable.poll(remoteLocalId);

        // send connection request to remote node via connection handler
        sendMessage(MessageType.HANDLER_REQ_CONNECTION, remoteHandlerTable.get(remoteLocalId), localQP.getPortNumber(), localQP.getLocalId(), localQP.getQueuePairNumber());

        // asynchronous check if request was received by remote
        dcm.executor.execute(new OutgoingMessageHandler(MessageType.HANDLER_REQ_CONNECTION, remoteLocalId, localQP.getPortNumber(), localQP.getLocalId(), localQP.getQueuePairNumber()));
        LOGGER.info("Initiate new reliable connection to {}", remoteLocalId);
    }

    /**
     * Send information about local RDMA buffer to a remote node
     *
     * @param bufferInformation the buffer information
     * @param localId           the local id of this node
     * @param remoteLocalId     the local id of the remote node
     */
    protected void sendBufferInfo(BufferInformation bufferInformation, short localId, short remoteLocalId) {
        // send information and check if received asynchronously
        dcm.executor.execute(new OutgoingMessageHandler(MessageType.HANDLER_SEND_BUFFER_INFO, remoteLocalId, localId, bufferInformation.getAddress(), bufferInformation.getCapacity(), bufferInformation.getRemoteKey()));
        LOGGER.trace("Send buffer info to {}", remoteLocalId);
    }

    /**
     * Initialize a disconnect request.
     *
     * @param localId       the local id of this node
     * @param remoteLocalId the local id of the remote node
     */
    protected void initDisconnectRequest(short localId, short remoteLocalId) {
        // handle sending and disconnecting asynchronously
        dcm.executor.execute(new OutgoingMessageHandler(MessageType.HANDLER_REQ_DISCONNECT, remoteLocalId, localId));
        LOGGER.trace("Send disconnect request to {}", remoteLocalId);
    }

    /**
     * Initialize a forced disconnect.
     *
     * @param localId       the local id of this node
     * @param remoteLocalId the local id of the remote node
     */
// handle with care - can have side effects!!
    protected void initDisconnectForce(short localId, short remoteLocalId) {
        // handle sending and disconnecting asynchronously
        dcm.executor.execute(new OutgoingMessageHandler(MessageType.HANDLER_SEND_DISCONNECT_FORCE, remoteLocalId, localId));
        LOGGER.trace("Send disconnect force to {}", remoteLocalId);
    }

    /**
     * Send an acknowledgement that buffer information was received
     *
     * @param localId       the local id of this node
     * @param remoteLocalId the local id of the remote node
     */
    protected void sendBufferAck(short localId, short remoteLocalId) {
        // handle sending asynchronously
        dcm.executor.execute(new OutgoingMessageHandler(MessageType.HANDLER_RESP_BUFFER_ACK, remoteLocalId, localId));
        LOGGER.trace("Send Buffer ack to {}", remoteLocalId);
    }

    /**
     * Send response to a connection request.
     *
     * @param localId       the local id of this node
     * @param remoteLocalId the local id of the remote node
     */
    protected void sendConnectionResponse(short localId, short remoteLocalId) {
        // handle sending asynchronously
        dcm.executor.execute(new OutgoingMessageHandler(MessageType.HANDLER_RESP_CONNECTION_REQ, remoteLocalId, localId));
        LOGGER.trace("Send responst to connection request to {}", remoteLocalId);
    }

    /**
     * Send a message to another handler.
     *
     * @param msgType    the message type
     * @param remoteInfo the remote info
     * @param payload    the payload of the message
     * @return the long
     */
    protected long sendMessage(MessageType msgType, UDInformation remoteInfo, long ... payload) {
        // get an id for the message
        var msgId = Message.provideGlobalId();

        // get a scatter gather element for the work request
        var sge = sendSGEProvider.getSGE();
        if(sge == null) {
            LOGGER.error("Cannot post another send request");
            return 0;
        }

        // create message into buffer referenced by the sge
        var msg = new LocalMessage(LocalBuffer.wrap(sge.getAddress(), sge.getLength()));
        msg.setMessageType(msgType);
        msg.setPayload(payload);
        msg.setId(msgId);

        // build the work request and put it into array
        var workRequest = buildSendWorkRequest(sge, remoteInfo, ownSendWrIdProvider.getAndIncrement() % MAX_SEND_WORK_REQUESTS);
        sendWorkRequests[(int) workRequest.getId()] = workRequest;

        // post the work request onto the ud queue pair
        postSend(workRequest);

        return msgId;
    }

    /**
     * Post receive work request to receive a message.
     */
    protected void receiveMessage() {
        // get a scatter gather element
        var sge = receiveSGEProvider.getSGE();
        if(sge == null) {
            LOGGER.error("Cannot post another receive request");
            return;
        }

        // build work request and put it into array
        var workRequest = buildReceiveWorkRequest(sge, ownReceiveWrIdProvider.getAndIncrement() % MAX_RECEIVE_WORK_REQUESTS);
        receiveWorkRequests[(int) workRequest.getId()] = workRequest;

        // post the work request onto the ud queue pair
        postReceive(workRequest);
    }

    /**
     * Shutdown the dynamic connection handler.
     */
    protected void shutdown() {
        // stop polling the completion queue
        udCompletionPoller.shutdown();

        // wait until all work completions are processed
        boolean killed = false;

        while(!killed) {
            killed = udCompletionPoller.isKilled();
        }
    }

    /**
     * Close the ud queue pair.
     */
    @Override
    public void close() {
        queuePair.close();
    }

    /**
     * Thread polling the completion queue of the dynamic connection handler.
     *
     * @author Christian Gesse
     */
    private class UDCompletionQueuePollThread extends Thread {

        /**
         * Indicator if thread should run any longer
         */
        private boolean isRunning = true;
        /**
         * Indicating if thread has finished processing
         */
        private boolean killed = false;
        /**
         * The size of the batchees which are polled from the completion queue
         */
        private final int batchSize = 20;

        /**
         * The run method of the thread.
         */
        @Override
        public void run() {

            LOGGER.trace("Fill up receive queue of dynamic connection handler");
            // initial fill up receive queue
            // we have to check fill count to prevent that Receive-WRs in array get overwritten
            while (receiveQueueFillCount.get() < MAX_RECEIVE_WORK_REQUESTS) {
                receiveMessage();
            }

            // create new work completion array
            var workCompletions = new CompletionQueue.WorkCompletionArray(batchSize);

            // start polling
            while(isRunning) {
                // poll a batch of send work completions
                sendCompletionQueue.poll(workCompletions);

                // process send work completions
                for(int i = 0; i < workCompletions.getLength(); i++) {
                    var completion = workCompletions.get(i);

                    // acknowledge send completion to queue pair
                    acknowledgeSendCompletion();

                    if(completion.getStatus() != WorkCompletion.Status.SUCCESS) {
                        LOGGER.error("Work completion failes with error [{}]: {}", completion.getStatus(), completion.getStatusMessage());
                    }

                    // extract scatter gather element and return it to provider
                    var sge = (ScatterGatherElement) NativeObjectRegistry.getObject(sendWorkRequests[(int) completion.getId()].getListHandle());
                    sendSGEProvider.returnSGE(sge);

                    // release work completion into buffer pool
                    sendWorkRequests[(int) completion.getId()].releaseInstance();
                    sendWorkRequests[(int) completion.getId()] = null;
                }

                // poll batch of receive completions
                receiveCompletionQueue.poll(workCompletions);

                // process receive work completions
                for(int i = 0; i < workCompletions.getLength(); i++) {
                    var completion = workCompletions.get(i);

                    // acknowledge receive completion to queue pair
                    acknowledgeReceiveCompletion();

                    if(completion.getStatus() != WorkCompletion.Status.SUCCESS) {
                        LOGGER.error("Work completion failes with error [{}]: {}", completion.getStatus(), completion.getStatusMessage());
                    }

                    // extract scatter gather element
                    var sge = (ScatterGatherElement) NativeObjectRegistry.getObject(receiveWorkRequests[(int) completion.getId()].getListHandle());
                    // extract buffer address and received message at this address
                    var message = new LocalMessage(LocalBuffer.wrap(sge.getAddress(), sge.getLength()), UnreliableDatagram.UD_Receive_Offset);

                    // process received message asynchronously
                    try {
                        dcm.executor.execute(new IncomingMessageHandler(message.getMessageType(), message.getId(), message.getPayload()));
                    } catch (RejectedExecutionException e) {
                        LOGGER.error("Executing task failed with exception: {}", e);
                    }

                    // return sge to provider
                    receiveSGEProvider.returnSGE(sge);

                    // return work request into buffer pool
                    receiveWorkRequests[(int) completion.getId()].releaseInstance();
                    receiveWorkRequests[(int) completion.getId()] = null;
                }

                // Fill up receive queue of dch to be ready to receive new messages
                while (receiveQueueFillCount.get() < MAX_RECEIVE_WORK_REQUESTS) {
                    receiveMessage();
                }
            }

            // if main loop breaks, the thread is killed
            killed = true;

        }

        /**
         * Shutdown poll thread.
         */
        public void shutdown() {
            isRunning = false;
        }

        /**
         * Check if poll thread is killed.
         *
         * @return the boolean indicating the status
         */
        public boolean isKilled() {
            return killed;
        }
    }

    /**
     * Handler for an received message. Is executed by a thread pool executor.
     *
     * @author Christian Gesse
     */
    private class IncomingMessageHandler implements Runnable {
        /**
         * The type of the received message
         */
        private final MessageType msgType;
        /**
         * The id of the received message
         */
        private final long msgId;
        /**
         * The payload of the received message
         */
        private final long[] payload;

        /**
         * Instantiates a new Incoming message handler.
         *
         * @param msgType the message type
         * @param msgId   the message id
         * @param payload the payload of the message
         */
        IncomingMessageHandler(MessageType msgType, long msgId, long... payload) {
            this.msgType = msgType;
            this.payload = payload;
            this.msgId = msgId;
        }

        /**
         * Run method.
         */
        @Override
        public void run() {

            // switch between the different message types and handle message
            switch(msgType) {
                case HANDLER_REQ_CONNECTION:
                    handleConnectionRequest();
                    break;
                case HANDLER_SEND_BUFFER_INFO:
                    handleBufferInfo();
                    break;
                case HANDLER_REQ_DISCONNECT:
                    handleDisconnectRequest();
                    break;
                case HANDLER_SEND_DISCONNECT_FORCE:
                    handleDisconnectForce();
                    break;
                case HANDLER_RESP_CONNECTION_REQ:
                    handleConnectionResponse();
                    break;
                case HANDLER_RESP_BUFFER_ACK:
                    handleBufferAck();
                    break;
                default:
                    LOGGER.info("Got message msgtype: {}, payload: {}", msgType, payload);
                    break;
            }

        }

        /**
         * Handle a connection request.
         */
        private void handleConnectionRequest() {
            // extract information about remote node from payload
            var remoteInfo = new RCInformation((byte) payload[0], (short) payload[1], (int) payload[2]);
            var remoteLocalId = remoteInfo.getLocalId();

            // send a response
            sendMessage(MessageType.HANDLER_RESP_CONNECTION_REQ, remoteHandlerTable.get(remoteLocalId), dcm.getLocalId());

            LOGGER.info("Got new connection request from {}", remoteInfo.getLocalId());

            boolean connected = false;

            // begin connecting process
            while (!connected) {
                // if necessary, create new queue pair for connection
                if (!dcm.connectionTable.containsKey(remoteLocalId)) {
                    dcm.createConnection(remoteLocalId);
                }

                // obtain read lock
                dcm.rwLocks.readLock(remoteLocalId);
                var connection = dcm.connectionTable.get(remoteLocalId);

                // try to connect the queue pair to the remote one
                try {
                    // returns boolean if a queue pair is connected or not
                    connected = connection.connect(remoteInfo);
                    // set time for latency measurement
                    dcm.statisticManager.endConnectLatencyStatistic(connection.getId(), System.nanoTime());
                } catch (Exception e) {
                    LOGGER.info("Could not connect to remote {}\n{}", remoteLocalId, e);
                } finally {
                    // release read lock
                    dcm.rwLocks.unlockRead(remoteLocalId);
                }
            }
        }

        /**
         * Handle a RDMA buffer information,
         */
        private void handleBufferInfo() {
            // extract remote buffer information from payload
            var bufferInfo = new BufferInformation(payload[1], payload[2], (int) payload[3]);
            var remoteLocalId = (short) payload[0];

            LOGGER.trace("Received new remote buffer information from {}: {}", remoteLocalId, bufferInfo);

            // register remote buffer for later use
            dcm.remoteBufferHandler.registerBufferInfo(remoteLocalId, bufferInfo);
            // acknowledge the buffer information
            sendBufferAck(dcm.getLocalId(), remoteLocalId);
        }

        /**
         * Handle acknowledgement of buffer information
         */
        private void handleBufferAck() {
            // extract local id of remote node
            var remoteLocalId = (short) payload[0];

            // change state of remoteStatus object
            if(remoteStatusMap.containsKey(remoteLocalId)) {
                var remoteStatus = remoteStatusMap.get(remoteLocalId);

                // amark buffer as acknowledged and wake up wating threads
                remoteStatus.bufferAck.set(true);
                remoteStatus.signalAll(remoteStatus.buffer);
            }
        }

        /**
         * Handle a disconnect request.
         */
        private void handleDisconnectRequest() {
            // extract local id of remote node
            var remoteLocalId = (int) payload[0];
            LOGGER.info("Received disconnect request from {}", remoteLocalId);

            // get the state of the remote node
            var remoteStatus = remoteStatusMap.get(remoteLocalId);

            // Begin disconnect if possible
            if(remoteStatus == null) {
                LOGGER.error("No remote status for {}", remoteLocalId);
            } else if(dcm.rcUsageTable.getStatus(remoteLocalId) == RCUsageTable.RC_UNSUSED && remoteStatus.disconnectAck.compareAndSet(RemoteStatus.DISCONNECT_NONE, RemoteStatus.DISCONNECT_ACCEPT)) {
                // try to obtain exclusive write lock
                var locked = dcm.rwLocks.tryWriteLock(remoteLocalId);
                // if successful and connection is unused, begin disconnect
                if(locked) {
                    if(dcm.rcUsageTable.getStatus(remoteLocalId) == RCUsageTable.RC_UNSUSED) {

                        // send a disconnect request as response
                        sendMessage(MessageType.HANDLER_REQ_DISCONNECT, remoteHandlerTable.get(remoteLocalId), dcm.getLocalId());

                        // disconnect the connection
                        var connection = dcm.connectionTable.get(remoteLocalId);

                        if(connection != null && connection.isConnected()) {
                            try {
                                LOGGER.info("Disconnect from {}", remoteLocalId);
                                var disconnected = connection.disconnect();

                                // close queue pair if disconnected succesfully
                                if(disconnected) {
                                    connection.close();
                                    dcm.connectionTable.remove(remoteLocalId);
                                    remoteStatus.resetConnection();
                                }
                            } catch (IOException e) {
                                LOGGER.error("Something went wrong during disconnect from {}" , remoteLocalId);
                            }
                        }

                    }
                    // release write lock
                    dcm.rwLocks.unlockWrite(remoteLocalId);
                }
                // reset remote status regarding disconnect
                remoteStatus.disconnectAck.set(RemoteStatus.DISCONNECT_NONE);

            } else if(remoteStatus.disconnectAck.compareAndSet(RemoteStatus.DISCONNECT_REQ, RemoteStatus.DISCONNECT_ACCEPT)) {
                // if another thread is already trying to disconnect, wakre up this thread
                LOGGER.info("Disconnect request from {} already pending - wake other thread", remoteLocalId);
                remoteStatus.signalAll(remoteStatus.disconnect);
            }
        }

        /**
         * Handle response to connection request.
         */
        private void handleConnectionResponse() {
            // extract local id of remote node
            var remoteLocalId = (short) payload[0];

            // change remote status and wake up waiting threads
            if(remoteStatusMap.containsKey(remoteLocalId)) {
                var remoteStatus = remoteStatusMap.get(remoteLocalId);

                remoteStatus.connectAck.set(true);
                remoteStatus.signalAll(remoteStatus.connect);
            }
        }

        /**
         * Handle a disconnect force.
         */
        private void handleDisconnectForce() {
            // extract local id of remote node
            var remoteLocalId = (short) payload[0];
            LOGGER.info("Got disconnect request from {}", remoteLocalId);

            // obtain write lock
            dcm.rwLocks.writeLock(remoteLocalId);
            var connection = dcm.connectionTable.get(remoteLocalId);

            // disconnect connection without further messaging
            if(connection != null) {
                try {
                    connection.forceDisconnect();
                    connection.close();

                    dcm.connectionTable.remove(remoteLocalId);
                } catch (IOException e) {
                    LOGGER.info("Disconnecting of connection {} failed: {}", connection.getId(), e);
                }
            }
            // release the exclusive write lock
            dcm.rwLocks.unlockWrite(remoteLocalId);
        }
    }

    /**
     * Handler for outgoing messages
     */
    private class OutgoingMessageHandler implements Runnable {
        /**
         * The type of the message
         */
        private final MessageType msgType;
        /**
         * The payload of the message
         */
        private final long[] payload;
        /**
         * The local id of the remote node
         */
        private final short remoteLocalId;

        /**
         * Instantiates a new Outgoing message handler.
         *
         * @param msgType       the message type
         * @param remoteLocalId the local id of the remote node
         * @param payload       the payload of the message
         */
        OutgoingMessageHandler(MessageType msgType, short remoteLocalId, long ... payload) {
            this.msgType = msgType;
            this.payload = payload;
            this.remoteLocalId = remoteLocalId;
        }

        /**
         * Run method of the thread.
         */
        @Override
        public void run() {

            // poll until remote UD pair data is available
            registeredHandlerTable.poll(remoteLocalId);

            // switch between message types and handle message
            switch(msgType) {
                case HANDLER_SEND_BUFFER_INFO:
                    handleBufferInfo();
                    break;
                case HANDLER_REQ_CONNECTION:
                    handleConnectionRequest();
                    break;
                case HANDLER_REQ_DISCONNECT:
                    handleDisconnectRequest();
                    break;
                case HANDLER_SEND_DISCONNECT_FORCE:
                    handleDisconnectForce();
                    break;
                case HANDLER_RESP_BUFFER_ACK:
                    handleBufferResponse();
                    break;
                default:
                    LOGGER.info("Could not send message: msgtype {}, payload {}", msgType, payload);
                    break;
            }
        }

        /**
         * Handle a RDMA buffer information.
         */
        private void handleBufferInfo() {

            boolean bufferAck = false;

            // create new remote status if necessary
            var remoteStatus = remoteStatusMap.putIfAbsent(remoteLocalId, new RemoteStatus());
            if(remoteStatus == null) {
                remoteStatus = remoteStatusMap.get(remoteLocalId);
            }

            // send buffer information until it is acknowledged by remote node
            while (!bufferAck) {
                // send message to remote
                sendMessage(msgType, remoteHandlerTable.get(remoteLocalId), payload);

                // wait for acknowledgement
                remoteStatus.waitForResponse(remoteStatus.buffer, RESP_BUFFER_ACK_TIMEOUT_MS);

                bufferAck = remoteStatus.bufferAck.get();
            }

        }

        /**
         * Handle a connection request
         */
        private void handleConnectionRequest() {
            boolean connectionAck = false;

            var remoteStatus = remoteStatusMap.get(remoteLocalId);

            // send connection request until it is acknowledged by remote node
            while (!connectionAck) {

                // wait for acknowledgement (one message was sent before this handler was started)
                remoteStatus.waitForResponse(remoteStatus.connect, RESP_CONNECTION_REQ_TIMEOUT_MS);

                // check remote status and send new connection request if necessary
                if(remoteStatus.connectAck.get()) {
                    connectionAck = true;
                } else {
                    sendMessage(msgType, remoteHandlerTable.get(remoteLocalId), payload);
                }
            }
        }

        /**
         * Handle disconnect request.
         */
        private void handleDisconnectRequest() {
            LOGGER.info("Begin disconnect request to {}", remoteLocalId);

            var remoteStatus = remoteStatusMap.get(remoteLocalId);

            // check if no other handler is trying to disconnect
            if(remoteStatus == null) {
                LOGGER.error("No remote status for {}" , remoteLocalId);
            } else if(remoteStatus.disconnectAck.compareAndSet(RemoteStatus.DISCONNECT_NONE, RemoteStatus.DISCONNECT_REQ)) {
                // obtain exclusive write lock
                var locked = dcm.rwLocks.tryWriteLock(remoteLocalId);

                if(locked) {

                    // send disconnect request to remote
                    sendMessage(MessageType.HANDLER_REQ_DISCONNECT, remoteHandlerTable.get(remoteLocalId), dcm.getLocalId());

                    // wait for a disconnect request as response from remote node
                    remoteStatus.waitForResponse(remoteStatus.disconnect, RESP_CONNECTION_REQ_TIMEOUT_MS);

                    // check if response was received
                    if(remoteStatus.disconnectAck.get() == RemoteStatus.DISCONNECT_ACCEPT) {

                        // continue disconnecting and close queue pair
                        var connection = dcm.connectionTable.get(remoteLocalId);

                        if(connection != null && connection.isConnected()) {
                            try {
                                var disconnected = connection.disconnect();

                                if(disconnected) {
                                    connection.close();
                                    dcm.connectionTable.remove(remoteLocalId);
                                    remoteStatus.resetConnection();
                                }
                            } catch (IOException e) {
                                LOGGER.error("Something went wrong during disconnect from {}" , remoteLocalId);
                            }

                        }
                    }
                    // relesa write lock
                    dcm.rwLocks.unlockWrite(remoteLocalId);
                }

                remoteStatus.disconnectAck.set(RemoteStatus.DISCONNECT_NONE);

            } else if(remoteStatus.disconnectAck.get() == RemoteStatus.DISCONNECT_REQ) {
                LOGGER.info("Do not start disconnect request {}: request already pending", remoteLocalId);
            }
        }

        /**
         * Handle a disconnect force.
         */
        private void handleDisconnectForce() {

            // obtain exclusive write lock
            dcm.rwLocks.writeLock(remoteLocalId);
            var connection = dcm.connectionTable.get(remoteLocalId);

            if(connection != null) {
                // send disconnect force message to remote node
                sendMessage(msgType, remoteHandlerTable.get(remoteLocalId), payload);

                // disconnect and close queue pair without further waiting
                try {
                    connection.forceDisconnect();
                    connection.close();

                    dcm.connectionTable.remove(remoteLocalId);

                } catch (IOException e) {
                    LOGGER.info("Disconnecting of connection {} failed: {}", connection.getId(), e);
                }
            }
            // release write lock
            dcm.rwLocks.unlockWrite(remoteLocalId);
        }

        /**
         * Handle a buffer response.
         */
        private void handleBufferResponse() {
            sendMessage(msgType, remoteHandlerTable.get(remoteLocalId), payload);
        }
    }

    /**
     * Status object for a remote node. Is used to store some information about the state of remote nodes.
     *
     * @author Christian Gesse
     */
    private class RemoteStatus {
        /**
         * Indicates that no disconnecting handler is active
         */
        public static final int DISCONNECT_NONE = 0;
        /**
         * Indicates that the disconnect request was acceptet by the remote node
         */
        public static final int DISCONNECT_ACCEPT = 1;
        /**
         * Indicates that anoter disconnect handler is active
         */
        public static final int DISCONNECT_REQ = 2;

        /**
         * The lock used for synchronization and signaling
         */
        public final ReentrantLock lock = new ReentrantLock();

        /**
         * A condition object on connect state
         */
        public final Condition connect = lock.newCondition();
        /**
         * A condition object on disconect state
         */
        public final Condition disconnect = lock.newCondition();
        /**
         * A condition object on state of RDMA buffer
         */
        public final Condition buffer = lock.newCondition();

        /**
         * Indicates if disconnect request was accepted
         */
        public final AtomicInteger disconnectAck = new AtomicInteger(DISCONNECT_NONE);
        /**
         * Indicates if connection request was accepted
         */
        public final AtomicBoolean connectAck = new AtomicBoolean(false);
        /**
         * Indicates if RDMA buffer information was received by remote node
         */
        public final AtomicBoolean bufferAck = new AtomicBoolean(false);

        /**
         * Wait for response on a certain condition
         *
         * @param condition the condition to wait on
         * @param timeoutMs the timeout in ms
         */
        public void waitForResponse(Condition condition, long timeoutMs) {
            lock.lock();

            try {
                condition.await(timeoutMs, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                lock.unlock();
            }
        }

        /**
         * Signal based on a certain condition
         *
         * @param condition the condition to signal on
         */
        public void signalAll(Condition condition) {
            lock.lock();

            condition.signalAll();

            lock.unlock();
        }

        /**
         * Reset connection state
         */
        public void resetConnection() {
            connectAck.set(false);
            disconnectAck.set(DISCONNECT_NONE);
        }
    }

    /**
     * A fast table that indicates if a remote connection handler is already registered.
     * Checking an atomic array is fatser than looking for the handler in the dynamic remote handler table
     *
     * @author Christian Gesse
     */
    private class RegisteredHandlerTable {
        /**
         * The standard time to wait during polling
         */
        private static final long POLL_NANOS = 1000;

        /**
         * Indicates that the remote connection handler is not registered
         */
        private static final int UNREGISTERED = 0;
        /**
         * Indicates that the remote connection handler is registered
         */
        private static final int REGISTERED = 1;

        /**
         * The table with one entry for each possible local id
         */
        private final AtomicIntegerArray table;
        /**
         * The size of the table
         */
        private final int size;

        /**
         * Instantiates a new Registered handler table.
         *
         * @param size the size of the table
         */
        public RegisteredHandlerTable(int size) {
            table = new AtomicIntegerArray(size);
            this.size = size;
        }

        /**
         * Registers a remote handler in this table
         *
         * @param i the local id
         */
        public void setRegistered(int i) {
            table.set(i, REGISTERED);
        }

        /**
         * Gets status of a remote handler in this table
         *
         * @param i the local id
         * @return indicator if handler is registered or not
         */
        public int getStatus(int i) {
            return table.get(i);
        }

        /**
         * Poll until a ceratin handler gets registered
         *
         * @param i the local id of the remote handler
         */
        public void poll(int i) {
            var status = table.get(i);

            // poll until registered
            while (status != REGISTERED) {
                status = table.get(i);

                LockSupport.parkNanos(POLL_NANOS);
            }
        }

        /**
         * Poll until a ceratin handler gets registered or timeout is reached
         *
         * @param i         the local id of the remote node
         * @param timeoutMs the timeout in ms
         * @return the boolean indicating if the remote handler is registered
         */
        public boolean poll(int i, long timeoutMs) {
            var status = table.get(i);

            // convert timeout to nanoseconds for more efficient polling
            var timeoutNs = TimeUnit.NANOSECONDS.convert(timeoutMs, TimeUnit.MILLISECONDS);
            var start = System.nanoTime();

            while (status != REGISTERED && System.nanoTime() - start < timeoutNs) {
                status = table.get(i);

                // park thread to reduce CPU load
                LockSupport.parkNanos(POLL_NANOS);
            }

            return status == REGISTERED;
        }
    }
}
