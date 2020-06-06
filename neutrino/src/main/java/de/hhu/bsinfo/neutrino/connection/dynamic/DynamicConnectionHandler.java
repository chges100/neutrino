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

            var workCompletions = new CompletionQueue.WorkCompletionArray(batchSize);

            while(isRunning) {

                sendCompletionQueue.poll(workCompletions);

                for(int i = 0; i < workCompletions.getLength(); i++) {
                    var completion = workCompletions.get(i);

                    acknowledgeSendCompletion();

                    if(completion.getStatus() != WorkCompletion.Status.SUCCESS) {
                        LOGGER.error("Work completion failes with error [{}]: {}", completion.getStatus(), completion.getStatusMessage());
                    }

                    var sge = (ScatterGatherElement) NativeObjectRegistry.getObject(sendWorkRequests[(int) completion.getId()].getListHandle());
                    sendSGEProvider.returnSGE(sge);

                    sendWorkRequests[(int) completion.getId()].releaseInstance();
                    sendWorkRequests[(int) completion.getId()] = null;
                }

                receiveCompletionQueue.poll(workCompletions);

                for(int i = 0; i < workCompletions.getLength(); i++) {
                    var completion = workCompletions.get(i);

                    acknowledgeReceiveCompletion();

                    if(completion.getStatus() != WorkCompletion.Status.SUCCESS) {
                        LOGGER.error("Work completion failes with error [{}]: {}", completion.getStatus(), completion.getStatusMessage());
                    }

                    var sge = (ScatterGatherElement) NativeObjectRegistry.getObject(receiveWorkRequests[(int) completion.getId()].getListHandle());

                    var message = new LocalMessage(LocalBuffer.wrap(sge.getAddress(), sge.getLength()), UnreliableDatagram.UD_Receive_Offset);

                    try {
                        dcm.executor.execute(new IncomingMessageHandler(message.getMessageType(), message.getId(), message.getPayload()));
                    } catch (RejectedExecutionException e) {
                        LOGGER.error("Executing task failed with exception: {}", e);
                    }

                    receiveSGEProvider.returnSGE(sge);

                    receiveWorkRequests[(int) completion.getId()].releaseInstance();
                    receiveWorkRequests[(int) completion.getId()] = null;
                }

                // Fill up receive queue of dch
                while (receiveQueueFillCount.get() < MAX_RECEIVE_WORK_REQUESTS) {
                    receiveMessage();
                }
            }

            killed = true;

        }

        /**
         * Shutdown.
         */
        public void shutdown() {
            isRunning = false;
        }

        /**
         * Is killed boolean.
         *
         * @return the boolean
         */
        public boolean isKilled() {
            return killed;
        }
    }


    private class IncomingMessageHandler implements Runnable {
        private final MessageType msgType;
        private final long msgId;
        private final long[] payload;

        /**
         * Instantiates a new Incoming message handler.
         *
         * @param msgType the msg type
         * @param msgId   the msg id
         * @param payload the payload
         */
        IncomingMessageHandler(MessageType msgType, long msgId, long... payload) {
            this.msgType = msgType;
            this.payload = payload;
            this.msgId = msgId;
        }

        @Override
        public void run() {
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

        private void handleConnectionRequest() {

            var remoteInfo = new RCInformation((byte) payload[0], (short) payload[1], (int) payload[2]);
            var remoteLocalId = remoteInfo.getLocalId();

            sendMessage(MessageType.HANDLER_RESP_CONNECTION_REQ, remoteHandlerTable.get(remoteLocalId), dcm.getLocalId());

            LOGGER.info("Got new connection request from {}", remoteInfo.getLocalId());

            boolean connected = false;

            while (!connected) {
                if (!dcm.connectionTable.containsKey(remoteLocalId)) {

                    dcm.createConnection(remoteLocalId);
                }

                dcm.rwLocks.readLock(remoteLocalId);
                var connection = dcm.connectionTable.get(remoteLocalId);

                try {
                    connected = connection.connect(remoteInfo);
                    dcm.statisticManager.endConnectLatencyStatistic(connection.getId(), System.nanoTime());
                } catch (Exception e) {
                    LOGGER.info("Could not connect to remote {}\n{}", remoteLocalId, e);
                } finally {
                    dcm.rwLocks.unlockRead(remoteLocalId);
                }
            }
        }

        private void handleBufferInfo() {
            var bufferInfo = new BufferInformation(payload[1], payload[2], (int) payload[3]);
            var remoteLocalId = (short) payload[0];

            LOGGER.trace("Received new remote buffer information from {}: {}", remoteLocalId, bufferInfo);

            dcm.remoteBufferHandler.registerBufferInfo(remoteLocalId, bufferInfo);

            sendBufferAck(dcm.getLocalId(), remoteLocalId);
        }

        private void handleBufferAck() {
            var remoteLocalId = (short) payload[0];

            if(remoteStatusMap.containsKey(remoteLocalId)) {
                var remoteStatus = remoteStatusMap.get(remoteLocalId);

                remoteStatus.bufferAck.set(true);
                remoteStatus.signalAll(remoteStatus.buffer);
            }
        }

        private void handleDisconnectRequest() {
            var remoteLocalId = (int) payload[0];
            LOGGER.info("Received disconnect request from {}", remoteLocalId);

            var remoteStatus = remoteStatusMap.get(remoteLocalId);

            if(remoteStatus == null) {
                LOGGER.error("No remote status for {}", remoteLocalId);
            } else if(dcm.rcUsageTable.getStatus(remoteLocalId) == RCUsageTable.RC_UNSUSED && remoteStatus.disconnectAck.compareAndSet(RemoteStatus.DISCONNECT_NONE, RemoteStatus.DISCONNECT_ACCEPT)) {

                var locked = dcm.rwLocks.tryWriteLock(remoteLocalId);

                if(locked) {
                    if(dcm.rcUsageTable.getStatus(remoteLocalId) == RCUsageTable.RC_UNSUSED) {

                        sendMessage(MessageType.HANDLER_REQ_DISCONNECT, remoteHandlerTable.get(remoteLocalId), dcm.getLocalId());

                        var connection = dcm.connectionTable.get(remoteLocalId);

                        if(connection != null && connection.isConnected()) {
                            try {
                                LOGGER.info("Disconnect from {}", remoteLocalId);
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
                    dcm.rwLocks.unlockWrite(remoteLocalId);
                }

                remoteStatus.disconnectAck.set(RemoteStatus.DISCONNECT_NONE);

            } else if(remoteStatus.disconnectAck.compareAndSet(RemoteStatus.DISCONNECT_REQ, RemoteStatus.DISCONNECT_ACCEPT)) {
                LOGGER.info("Disconnect request from {} already pending - wake other thread", remoteLocalId);
                remoteStatus.signalAll(remoteStatus.disconnect);
            }
        }

        private void handleConnectionResponse() {
            var remoteLocalId = (short) payload[0];

            if(remoteStatusMap.containsKey(remoteLocalId)) {
                var remoteStatus = remoteStatusMap.get(remoteLocalId);

                remoteStatus.connectAck.set(true);
                remoteStatus.signalAll(remoteStatus.connect);
            }
        }

        private void handleDisconnectForce() {
            var remoteLocalId = (short) payload[0];
            LOGGER.info("Got disconnect request from {}", remoteLocalId);

            dcm.rwLocks.writeLock(remoteLocalId);
            var connection = dcm.connectionTable.get(remoteLocalId);

            if(connection != null) {
                try {
                    connection.forceDisconnect();
                    connection.close();

                    dcm.connectionTable.remove(remoteLocalId);
                } catch (IOException e) {
                    LOGGER.info("Disconnecting of connection {} failed: {}", connection.getId(), e);
                }
            }

            dcm.rwLocks.unlockWrite(remoteLocalId);
        }
    }

    private class OutgoingMessageHandler implements Runnable {
        private final MessageType msgType;
        private final long[] payload;
        private final short remoteLocalId;

        /**
         * Instantiates a new Outgoing message handler.
         *
         * @param msgType       the msg type
         * @param remoteLocalId the remote local id
         * @param payload       the payload
         */
        OutgoingMessageHandler(MessageType msgType, short remoteLocalId, long ... payload) {
            this.msgType = msgType;
            this.payload = payload;
            this.remoteLocalId = remoteLocalId;
        }

        @Override
        public void run() {

            // poll until remote UD pair data is available
            registeredHandlerTable.poll(remoteLocalId);

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

        private void handleBufferInfo() {

            boolean bufferAck = false;

            var remoteStatus = remoteStatusMap.putIfAbsent(remoteLocalId, new RemoteStatus());
            if(remoteStatus == null) {
                remoteStatus = remoteStatusMap.get(remoteLocalId);
            }

            while (!bufferAck) {

                sendMessage(msgType, remoteHandlerTable.get(remoteLocalId), payload);

                remoteStatus.waitForResponse(remoteStatus.buffer, RESP_BUFFER_ACK_TIMEOUT_MS);

                bufferAck = remoteStatus.bufferAck.get();
            }

        }

        private void handleConnectionRequest() {
            boolean connectionAck = false;

            var remoteStatus = remoteStatusMap.get(remoteLocalId);

            while (!connectionAck) {

                remoteStatus.waitForResponse(remoteStatus.connect, RESP_CONNECTION_REQ_TIMEOUT_MS);

                if(remoteStatus.connectAck.get()) {
                    connectionAck = true;
                } else {
                    sendMessage(msgType, remoteHandlerTable.get(remoteLocalId), payload);
                }
            }
        }

        private void handleDisconnectRequest() {
            LOGGER.info("Begin disconnect request to {}", remoteLocalId);

            var remoteStatus = remoteStatusMap.get(remoteLocalId);

            if(remoteStatus == null) {
                LOGGER.error("No remote status for {}" , remoteLocalId);
            } else if(remoteStatus.disconnectAck.compareAndSet(RemoteStatus.DISCONNECT_NONE, RemoteStatus.DISCONNECT_REQ)) {

                var locked = dcm.rwLocks.tryWriteLock(remoteLocalId);

                if(locked) {
                    LOGGER.debug("GOT lock: can start disconnect from {}", remoteLocalId);

                    sendMessage(MessageType.HANDLER_REQ_DISCONNECT, remoteHandlerTable.get(remoteLocalId), dcm.getLocalId());

                    LOGGER.info("Wait for answer to disconnect request {}", remoteLocalId);

                    remoteStatus.waitForResponse(remoteStatus.disconnect, RESP_CONNECTION_REQ_TIMEOUT_MS);

                    if(remoteStatus.disconnectAck.get() == RemoteStatus.DISCONNECT_ACCEPT) {
                        LOGGER.info("DISCONNECt accepted: {}" ,remoteLocalId);
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

                    dcm.rwLocks.unlockWrite(remoteLocalId);
                }

                remoteStatus.disconnectAck.set(RemoteStatus.DISCONNECT_NONE);

            } else if(remoteStatus.disconnectAck.get() == RemoteStatus.DISCONNECT_REQ) {
                LOGGER.info("Do not start disconnect request {}: request already pending", remoteLocalId);
            }
        }

        private void handleDisconnectForce() {

            dcm.rwLocks.writeLock(remoteLocalId);
            var connection = dcm.connectionTable.get(remoteLocalId);

            if(connection != null) {

                sendMessage(msgType, remoteHandlerTable.get(remoteLocalId), payload);

                try {
                    connection.forceDisconnect();
                    connection.close();

                    dcm.connectionTable.remove(remoteLocalId);

                } catch (IOException e) {
                    LOGGER.info("Disconnecting of connection {} failed: {}", connection.getId(), e);
                }
            }
            dcm.rwLocks.unlockWrite(remoteLocalId);
        }

        private void handleBufferResponse() {
            sendMessage(msgType, remoteHandlerTable.get(remoteLocalId), payload);
        }
    }

    private class RemoteStatus {
        /**
         * The constant DISCONNECT_NONE.
         */
        public static final int DISCONNECT_NONE = 0;
        /**
         * The constant DISCONNECT_ACCEPT.
         */
        public static final int DISCONNECT_ACCEPT = 1;
        /**
         * The constant DISCONNECT_REQ.
         */
        public static final int DISCONNECT_REQ = 2;

        /**
         * The Lock.
         */
        public final ReentrantLock lock = new ReentrantLock();

        /**
         * The Connect.
         */
        public final Condition connect = lock.newCondition();
        /**
         * The Disconnect.
         */
        public final Condition disconnect = lock.newCondition();
        /**
         * The Buffer.
         */
        public final Condition buffer = lock.newCondition();

        /**
         * The Disconnect ack.
         */
        public final AtomicInteger disconnectAck = new AtomicInteger(DISCONNECT_NONE);
        /**
         * The Connect ack.
         */
        public final AtomicBoolean connectAck = new AtomicBoolean(false);
        /**
         * The Buffer ack.
         */
        public final AtomicBoolean bufferAck = new AtomicBoolean(false);

        /**
         * Wait for response.
         *
         * @param condition the condition
         * @param timeoutMs the timeout ms
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
         * Signal all.
         *
         * @param condition the condition
         */
        public void signalAll(Condition condition) {
            lock.lock();

            condition.signalAll();

            lock.unlock();
        }

        /**
         * Reset connection.
         */
        public void resetConnection() {
            connectAck.set(false);
            disconnectAck.set(DISCONNECT_NONE);
        }
    }

    private class RegisteredHandlerTable {
        private static final long POLL_NANOS = 1000;

        private static final int UNREGISTERED = 0;
        private static final int REGISTERED = 1;

        private final AtomicIntegerArray table;
        private final int size;

        /**
         * Instantiates a new Registered handler table.
         *
         * @param size the size
         */
        public RegisteredHandlerTable(int size) {
            table = new AtomicIntegerArray(size);
            this.size = size;
        }

        /**
         * Sets registered.
         *
         * @param i the
         */
        public void setRegistered(int i) {
            table.set(i, REGISTERED);
        }

        /**
         * Gets status.
         *
         * @param i the
         * @return the status
         */
        public int getStatus(int i) {
            return table.get(i);
        }

        /**
         * Poll.
         *
         * @param i the
         */
        public void poll(int i) {
            var status = table.get(i);

            while (status != REGISTERED) {
                status = table.get(i);

                LockSupport.parkNanos(POLL_NANOS);
            }
        }

        /**
         * Poll boolean.
         *
         * @param i         the
         * @param timeoutMs the timeout ms
         * @return the boolean
         */
        public boolean poll(int i, long timeoutMs) {
            var status = table.get(i);

            var timeoutNs = TimeUnit.NANOSECONDS.convert(timeoutMs, TimeUnit.MILLISECONDS);
            var start = System.nanoTime();

            while (status != REGISTERED && System.nanoTime() - start < timeoutNs) {
                status = table.get(i);

                LockSupport.parkNanos(POLL_NANOS);
            }

            return status == REGISTERED;
        }
    }
}
