package de.hhu.bsinfo.neutrino.connection.dynamic;

import de.hhu.bsinfo.neutrino.buffer.BufferInformation;
import de.hhu.bsinfo.neutrino.buffer.LocalBuffer;
import de.hhu.bsinfo.neutrino.connection.DeviceContext;
import de.hhu.bsinfo.neutrino.connection.UnreliableDatagram;
import de.hhu.bsinfo.neutrino.connection.message.LocalMessage;
import de.hhu.bsinfo.neutrino.connection.message.Message;
import de.hhu.bsinfo.neutrino.connection.message.MessageType;
import de.hhu.bsinfo.neutrino.connection.util.ConcurrentRingBufferPool;
import de.hhu.bsinfo.neutrino.connection.util.RCInformation;
import de.hhu.bsinfo.neutrino.connection.util.SGEProvider;
import de.hhu.bsinfo.neutrino.connection.util.UDInformation;
import de.hhu.bsinfo.neutrino.util.NativeObjectRegistry;
import de.hhu.bsinfo.neutrino.util.Poolable;
import de.hhu.bsinfo.neutrino.verbs.ReceiveWorkRequest;
import de.hhu.bsinfo.neutrino.verbs.ScatterGatherElement;
import de.hhu.bsinfo.neutrino.verbs.SendWorkRequest;
import de.hhu.bsinfo.neutrino.verbs.WorkCompletion;
import org.agrona.collections.Int2ObjectHashMap;
import org.jctools.maps.NonBlockingHashMapLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.locks.LockSupport;

public final class DynamicConnectionHandler extends UnreliableDatagram {
    private static final Logger LOGGER = LoggerFactory.getLogger(DynamicConnectionHandler.class);

    private static final int MAX_SEND_WORK_REQUESTS = 500;
    private static final int MAX_RECEIVE_WORK_REQUESTS = 500;

    private static final int SEND_COMPLETION_QUEUE_SIZE = 1000;
    private static final int RECEIVE_COMPLETION_QUEUE_SIZE = 1000;

    private static final long RW_LOCK_TIMEOUT_MS = 100;
    private static final long RESP_BUFFER_ACK_TIMEOUT_MS = 200;
    private static final long RESP_DISCONNECT_REQ_TIMEOUT_MS = 200;
    private static final long RESP_DISCONNECT_FORCE_RESP_TIMEOUT_MS = 400;

    private static final int RING_BUFF_POOL_SIZE = 100;

    private final DynamicConnectionManager dcm;
    private final Int2ObjectHashMap<UDInformation> remoteHandlerTables;
    private final RegisteredHandlerTable registeredHandlerTable;
    private final ThreadPoolExecutor executor;

    private final UDInformation localUDInformation;

    private final AtomicInteger receiveQueueFillCount;

    private final SendWorkRequest[] sendWorkRequests;
    private final ReceiveWorkRequest[] receiveWorkRequests;

    private final AtomicInteger ownSendWrIdProvider;
    private final AtomicInteger ownReceiveWrIdProvider;

    private final NonBlockingHashMapLong<StatusObject> callbackMap;
    private final ConcurrentRingBufferPool<StatusObject> statusObjectPool;


    private final SGEProvider sendSGEProvider;
    private final SGEProvider receiveSGEProvider;

    private final UDCompletionQueuePollThread udCompletionPoller;

    protected DynamicConnectionHandler(DynamicConnectionManager dcm, DeviceContext deviceContext, int maxLid) throws IOException {

        super(deviceContext);

        this.dcm = dcm;

        LOGGER.info("Set up dynamic connection handler");

        remoteHandlerTables = new Int2ObjectHashMap<>();
        registeredHandlerTable = new RegisteredHandlerTable(maxLid);
        executor = (ThreadPoolExecutor) Executors.newCachedThreadPool();

        ownSendWrIdProvider = new AtomicInteger(0);
        ownReceiveWrIdProvider = new AtomicInteger(0);

        receiveQueueFillCount = new AtomicInteger(0);

        callbackMap = new NonBlockingHashMapLong<>();
        statusObjectPool = new ConcurrentRingBufferPool<>(RING_BUFF_POOL_SIZE, StatusObject::new);

        sendWorkRequests = new SendWorkRequest[MAX_SEND_WORK_REQUESTS];
        receiveWorkRequests = new ReceiveWorkRequest[MAX_RECEIVE_WORK_REQUESTS];

        sendSGEProvider = new SGEProvider(getDeviceContext(), MAX_SEND_WORK_REQUESTS, Message.getSize());
        receiveSGEProvider = new SGEProvider(getDeviceContext(), MAX_RECEIVE_WORK_REQUESTS, Message.getSize() + UD_Receive_Offset);

        init();

        localUDInformation = new UDInformation((byte) 1, getPortAttributes().getLocalId(), getQueuePair().getQueuePairNumber(),
                getQueuePair().queryAttributes().getQkey());

        udCompletionPoller = new UDCompletionQueuePollThread();
        udCompletionPoller.start();
    }

    protected UDInformation getLocalUDInformation() {
        return localUDInformation;
    }

    protected void registerRemoteConnectionHandler(int remoteLocalId, UDInformation remoteHandlerInfo) {
        remoteHandlerTables.put(remoteLocalId, remoteHandlerInfo);

        registeredHandlerTable.setRegistered(remoteLocalId);
    }

    protected UDInformation getRemoteHandlerInfo(int remoteLocalId) {
        return remoteHandlerTables.get(remoteLocalId);
    }

    protected boolean hasRemoteHandlerInfo(int remoteLocalId) {
        return remoteHandlerTables.containsKey(remoteLocalId);
    }

    protected short[] getRemoteLocalIds() {
        var n = remoteHandlerTables.size();
        var remotes = remoteHandlerTables.keySet().toArray();
        short[] lids = new short[n];

        for(int i = 0; i < lids.length; i++) {
            lids[i] =  (short) (int) remotes[i];
        }

        return lids;
    }

    protected void sendConnectionRequest(RCInformation localQP, short remoteLocalId) {
        executor.submit(new OutgoingMessageHandler(MessageType.HANDLER_REQ_CONNECTION, remoteLocalId, localQP.getPortNumber(), localQP.getLocalId(), localQP.getQueuePairNumber()));
        LOGGER.info("Initiate new reliable connection to {}", remoteLocalId);
    }

    protected void sendBufferInfo(BufferInformation bufferInformation, short localId, short remoteLocalId) {
        executor.submit(new OutgoingMessageHandler(MessageType.HANDLER_SEND_BUFFER_INFO, remoteLocalId, localId, bufferInformation.getAddress(), bufferInformation.getCapacity(), bufferInformation.getRemoteKey()));
        LOGGER.trace("Send buffer info to {}", remoteLocalId);
    }

    protected void sendDisconnectRequest(short localId, short remoteLocalId) {
        executor.submit(new OutgoingMessageHandler(MessageType.HANDLER_REQ_DISCONNECT, remoteLocalId, localId));
        LOGGER.trace("Send disconnect request to {}", remoteLocalId);
    }

    protected void sendDisconnectForce(short localId, short remoteLocalId) {
        executor.submit(new OutgoingMessageHandler(MessageType.HANDLER_SEND_DISCONNECT_FORCE, remoteLocalId, localId));
        LOGGER.trace("Send disconnect force to {}", remoteLocalId);
    }

    protected void sendBufferAck(short localId, long msgId, short remoteLocalId) {
        executor.submit(new OutgoingMessageHandler(MessageType.HANDLER_RESP_BUFFER_ACK, remoteLocalId, msgId, localId));
        LOGGER.trace("Send Buffer ack to {}", remoteLocalId);
    }

    protected void sendDisconnectResponse(long msgId, long response, short remoteLocalId) {
        executor.submit(new OutgoingMessageHandler(MessageType.HANDLER_RESP_DISCONNECT_REQ, remoteLocalId, msgId, response));
        LOGGER.trace("Send disconnect request to {}", remoteLocalId);
    }

    protected void sendMessage(MessageType msgType, long msgId, UDInformation remoteInfo, long ... payload) {
        var sge = sendSGEProvider.getSGE();
        if(sge == null) {
            LOGGER.error("Cannot post another send request");
            return;
        }

        var msg = new LocalMessage(LocalBuffer.wrap(sge.getAddress(), sge.getLength()));
        msg.setMessageType(msgType);
        msg.setPayload(payload);
        msg.setId(msgId);


        var workRequest = buildSendWorkRequest(sge, remoteInfo, ownSendWrIdProvider.getAndIncrement() % MAX_SEND_WORK_REQUESTS);
        sendWorkRequests[(int) workRequest.getId()] = workRequest;

        postSend(workRequest);
    }

    protected long sendMessage(MessageType msgType, UDInformation remoteInfo, long ... payload) {
        var msgId = Message.provideGlobalId();

        sendMessage(msgType, msgId, remoteInfo, payload);

        return msgId;
    }

    protected void receiveMessage() {
        var sge = receiveSGEProvider.getSGE();
        if(sge == null) {
            LOGGER.error("Cannot post another receive request");
            return;
        }

        var workRequest = buildReceiveWorkRequest(sge, ownReceiveWrIdProvider.getAndIncrement() % MAX_RECEIVE_WORK_REQUESTS);
        receiveWorkRequests[(int) workRequest.getId()] = workRequest;

        postReceive(workRequest);
    }

    protected void shutdown() {
        executor.shutdownNow();

        try {
            executor.awaitTermination(500, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            LOGGER.info("Thread Pool termination not yet finished - continue shutdown");
        }

        LOGGER.debug("Executor is shut down");

        close();
    }

    @Override
    public void close() {
        udCompletionPoller.shutdown();

        boolean killed = false;

        while(!killed) {
            killed = udCompletionPoller.isKilled();
        }

        queuePair.close();
    }

    private class UDCompletionQueuePollThread extends Thread {

        private boolean isRunning = true;
        private boolean killed = false;
        private final int batchSize = 10;

        @Override
        public void run() {

            LOGGER.info("Fill up receive queue of dynamic connection handler");
            // initial fill up receive queue
            while (receiveQueueFillCount.get() < receiveQueueSize) {
                receiveMessage();
                receiveQueueFillCount.incrementAndGet();
            }

            while(isRunning) {

                var workCompletions = pollSendCompletions(batchSize);

                for(int i = 0; i < workCompletions.getLength(); i++) {
                    var completion = workCompletions.get(i);

                    if(completion.getStatus() != WorkCompletion.Status.SUCCESS) {
                        LOGGER.error("Work completion failes with error [{}]: {}", completion.getStatus(), completion.getStatusMessage());
                    }

                    var sge = (ScatterGatherElement) NativeObjectRegistry.getObject(sendWorkRequests[(int) completion.getId()].getListHandle());
                    sendSGEProvider.returnSGE(sge);

                    sendWorkRequests[(int) completion.getId()].releaseInstance();
                }

                workCompletions = pollReceiveCompletions(batchSize);

                for(int i = 0; i < workCompletions.getLength(); i++) {
                    var completion = workCompletions.get(i);

                    if(completion.getStatus() != WorkCompletion.Status.SUCCESS) {
                        LOGGER.error("Work completion failes with error [{}]: {}", completion.getStatus(), completion.getStatusMessage());
                    }

                    receiveQueueFillCount.decrementAndGet();

                    var sge = (ScatterGatherElement) NativeObjectRegistry.getObject(receiveWorkRequests[(int) completion.getId()].getListHandle());

                    var message = new LocalMessage(LocalBuffer.wrap(sge.getAddress(), sge.getLength()), UnreliableDatagram.UD_Receive_Offset);

                    try {
                        executor.submit(new IncomingMessageHandler(message.getMessageType(), message.getId(), message.getPayload()));
                    } catch (RejectedExecutionException e) {
                        LOGGER.error("Submitting task failed with exception: {}", e);
                    }

                    receiveSGEProvider.returnSGE(sge);

                    receiveWorkRequests[(int) completion.getId()].releaseInstance();
                }

                // Fill up receive queue of dch
                while (receiveQueueFillCount.get() < receiveQueueSize) {
                    receiveMessage();
                    receiveQueueFillCount.incrementAndGet();
                }
            }

            killed = true;

        }

        public void shutdown() {
            isRunning = false;
        }

        public boolean isKilled() {
            return killed;
        }
    }


    private class IncomingMessageHandler implements Runnable {
        private final MessageType msgType;
        private final long msgId;
        private final long[] payload;

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
                case HANDLER_RESP_DISCONNECT_REQ:
                    handleDisconnectResponse();
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
                } catch (Exception e) {
                    LOGGER.debug("Could not connect to remote {}\n{}", remoteLocalId, e);
                } finally {
                    dcm.rwLocks.unlockRead(remoteLocalId);
                }
            }

        }

        private void handleBufferInfo() {
            var bufferInfo = new BufferInformation(payload[1], payload[2], (int) payload[3]);
            var remoteLid = (short) payload[0];

            LOGGER.trace("Received new remote buffer information from {}: {}", remoteLid, bufferInfo);

            dcm.remoteBufferHandler.registerBufferInfo(remoteLid, bufferInfo);

            sendBufferAck(dcm.getLocalId(), msgId, remoteLid);
        }

        private void handleBufferAck() {
            var oldMsgId = payload[0];
            var remoteLid = (short) payload[1];

            if(callbackMap.containsKey(oldMsgId)) {
                var statusObject = callbackMap.get(oldMsgId);

                statusObject.setBufferAckAndNotify();
            }
        }

        private void handleDisconnectRequest() {
            var remoteLid = (int) payload[0];
            LOGGER.info("Got disconnect request from {}", remoteLid);

            boolean disconnect = false;

            if(dcm.rcUsageTable.getStatus(remoteLid) == RCUsageTable.RC_UNSUSED) {
                // try to lock once - if lock is not available, connection is in use an  no disconnecting should occur
                var locked = dcm.rwLocks.tryWriteLock(remoteLid);
                var connection = dcm.connectionTable.get(remoteLid);

                if(locked && connection != null && connection.isConnected()) {
                    sendDisconnectResponse(msgId, StatusObject.DISCONNECT_ACCEPT, (short) remoteLid);

                    try {
                        disconnect = true;
                        connection.disconnect();
                        connection.close();

                        dcm.connectionTable.remove(remoteLid);
                    } catch (IOException e) {
                        LOGGER.info("Disconnecting of connection {} failed: {}", connection.getId(), e);
                    }

                    dcm.rwLocks.unlockWrite(remoteLid);
                }

                if(!disconnect) {
                    sendDisconnectResponse(msgId, StatusObject.DISCONNECT_REJECT, (short) remoteLid);
                }
            }
        }

        private void handleDisconnectResponse() {
            var oldMsgId = payload[0];
            var response = (int) payload[1];

            if(callbackMap.containsKey(oldMsgId)) {
                var statusObject = callbackMap.get(oldMsgId);

                if(response == StatusObject.DISCONNECT_ACCEPT) {
                    statusObject.setDisconnectAcceptAndNotify();
                } else {
                    statusObject.setDisconnectRejectAndNotify();
                }
            }
        }

        private void handleDisconnectForce() {
            var remoteLid = (short) payload[0];
            LOGGER.info("Got disconnect request from {}", remoteLid);

            dcm.rwLocks.writeLock(remoteLid);
            var connection = dcm.connectionTable.get(remoteLid);

            if(connection != null) {
                try {
                    connection.disconnect();
                    connection.close();

                    dcm.connectionTable.remove(remoteLid);
                } catch (IOException e) {
                    LOGGER.info("Disconnecting of connection {} failed: {}", connection.getId(), e);
                }
            }

            dcm.rwLocks.unlockWrite(remoteLid);
        }
    }

    private class OutgoingMessageHandler implements Runnable {
        private final MessageType msgType;
        private final long[] payload;
        private final short remoteLocalId;

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
                case HANDLER_RESP_DISCONNECT_REQ:
                    handleDisconnectResponse();
                    break;
                default:
                    LOGGER.info("Could not send message: msgtype {}, payload {}", msgType, payload);
                    break;
            }
        }

        private void handleBufferInfo() {

            boolean bufferAck = false;

            while (!bufferAck) {
                var msgId = Message.provideGlobalId();

                var statusObject = statusObjectPool.getInstance();
                callbackMap.put(msgId, statusObject);

                sendMessage(msgType, msgId, remoteHandlerTables.get(remoteLocalId), payload);

                statusObject.waitForResponse(RESP_BUFFER_ACK_TIMEOUT_MS);

                if(statusObject.getStatus() == StatusObject.BUFFER_ACK) {
                    bufferAck = true;
                }

                callbackMap.remove(msgId, statusObject);
                statusObject.releaseInstance();
            }

        }

        private void handleConnectionRequest() {
            sendMessage(msgType, remoteHandlerTables.get(remoteLocalId), payload);
        }

        private void handleDisconnectRequest() {

            // try to lock once - if lock is not available, connection is in use an  no disconnecting should occur
            var locked = dcm.rwLocks.tryWriteLock(remoteLocalId);

            if(locked) {
                var msgId = Message.provideGlobalId();

                var statusObject = statusObjectPool.getInstance();
                callbackMap.put(msgId, statusObject);

                sendMessage(msgType, msgId, remoteHandlerTables.get(remoteLocalId), payload);

                statusObject.waitForResponse(RESP_DISCONNECT_REQ_TIMEOUT_MS);

                if(statusObject.getStatus() == StatusObject.DISCONNECT_ACCEPT) {
                    var connection = dcm.connectionTable.get(remoteLocalId);

                    if(connection != null && connection.isConnected()) {
                        try {
                            connection.disconnect();
                            connection.close();

                            dcm.connectionTable.remove(remoteLocalId);
                        } catch (IOException e) {
                            LOGGER.info("Disconnecting of connection {} failed: {}", connection.getId(), e);
                        }
                    }
                }

                dcm.rwLocks.unlockWrite(remoteLocalId);

                callbackMap.remove(msgId);
                statusObject.releaseInstance();
            }
        }

        private void handleDisconnectForce() {

            dcm.rwLocks.writeLock(remoteLocalId);
            var connection = dcm.connectionTable.get(remoteLocalId);

            if(connection != null) {

                sendMessage(msgType, remoteHandlerTables.get(remoteLocalId), payload);

                try {
                    connection.disconnect();
                    connection.close();

                    dcm.connectionTable.remove(remoteLocalId);

                } catch (IOException e) {
                    LOGGER.info("Disconnecting of connection {} failed: {}", connection.getId(), e);
                }
            }
            dcm.rwLocks.unlockWrite(remoteLocalId);
        }

        private void handleDisconnectResponse() {
            sendMessage(msgType, remoteHandlerTables.get(remoteLocalId), payload);
        }

        private void handleBufferResponse() {
            sendMessage(msgType, remoteHandlerTables.get(remoteLocalId), payload);
        }
    }

    private class StatusObject implements Poolable {
        public static final int STATELESS = 0;
        public static final int BUFFER_ACK = 1;
        public static final int DISCONNECT_ACCEPT = 2;
        public static final int DISCONNECT_REJECT = 3;

        private AtomicInteger status = new AtomicInteger(0);

        private void setStatus(int status) {
            this.status.set(status);
        }

        public void setStateless() {
            setStatus(STATELESS);
        }

        public synchronized void setBufferAckAndNotify() {
            setStatus(BUFFER_ACK);

            notifyAll();
        }

        public synchronized void setDisconnectAcceptAndNotify() {
            setStatus(DISCONNECT_ACCEPT);

            notifyAll();
        }

        public synchronized void setDisconnectRejectAndNotify() {
            setStatus(DISCONNECT_REJECT);

            notifyAll();
        }

        public int getStatus() {
            return status.get();
        }

        public synchronized void waitForResponse(long timeout) {
            try {
                wait(timeout);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        @Override
        public synchronized void releaseInstance() {
            status.set(0);
            statusObjectPool.returnInstance(this);
        }
    }

    private class RegisteredHandlerTable {
        private static final long POLL_NANOS = 1000;

        private static final int UNREGISTERED = 0;
        private static final int REGISTERED = 1;

        private final AtomicIntegerArray table;
        private final int size;

        public RegisteredHandlerTable(int size) {
            table = new AtomicIntegerArray(size);
            this.size = size;
        }

        public void setRegistered(int i) {
            table.set(i, REGISTERED);
        }

        public int getStatus(int i) {
            return table.get(i);
        }

        public void poll(int i) {
            var status = table.get(i);

            while (status != REGISTERED) {
                status = table.get(i);

                LockSupport.parkNanos(POLL_NANOS);
            }
        }

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
