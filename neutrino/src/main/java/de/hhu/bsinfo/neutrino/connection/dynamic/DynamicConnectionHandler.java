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
import de.hhu.bsinfo.neutrino.verbs.ReceiveWorkRequest;
import de.hhu.bsinfo.neutrino.verbs.ScatterGatherElement;
import de.hhu.bsinfo.neutrino.verbs.SendWorkRequest;
import de.hhu.bsinfo.neutrino.verbs.WorkCompletion;
import org.agrona.collections.Int2ObjectHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.LockSupport;

public final class DynamicConnectionHandler extends UnreliableDatagram {
    private static final Logger LOGGER = LoggerFactory.getLogger(DynamicConnectionHandler.class);

    private static final int MAX_SEND_WORK_REQUESTS = 500;
    private static final int MAX_RECEIVE_WORK_REQUESTS = 500;

    private static final long BUFFER_ACK_TIMEOUT = 200;

    private final DynamicConnectionManager dcm;
    private final Int2ObjectHashMap<UDInformation> remoteHandlerInfos;
    private final ThreadPoolExecutor executor;

    private final UDInformation localUDInformation;

    private final AtomicInteger receiveQueueFillCount;

    private final SendWorkRequest sendWorkRequests[];
    private final ReceiveWorkRequest receiveWorkRequests[];


    private final SGEProvider sendSGEProvider;
    private final SGEProvider receiveSGEProvider;

    private final UDCompletionQueuePollThread udCqpt;

    protected DynamicConnectionHandler(DynamicConnectionManager dcm, DeviceContext deviceContext) throws IOException {

        super(deviceContext);

        this.dcm = dcm;

        LOGGER.info("Set up dynamic connection handler");

        remoteHandlerInfos = new Int2ObjectHashMap<>();
        executor = (ThreadPoolExecutor) Executors.newCachedThreadPool();

        receiveQueueFillCount = new AtomicInteger(0);

        sendWorkRequests = new SendWorkRequest[MAX_SEND_WORK_REQUESTS];
        receiveWorkRequests = new ReceiveWorkRequest[MAX_RECEIVE_WORK_REQUESTS];

        sendSGEProvider = new SGEProvider(getDeviceContext(), MAX_SEND_WORK_REQUESTS, Message.getSize());
        receiveSGEProvider = new SGEProvider(getDeviceContext(), MAX_RECEIVE_WORK_REQUESTS, Message.getSize() + UD_Receive_Offset);

        init();

        localUDInformation = new UDInformation((byte) 1, getPortAttributes().getLocalId(), getQueuePair().getQueuePairNumber(),
                getQueuePair().queryAttributes().getQkey());

        udCqpt = new UDCompletionQueuePollThread();
        udCqpt.start();
    }

    protected UDInformation getLocalUDInformation() {
        return localUDInformation;
    }

    protected void registerRemoteConnectionHandler(int remoteLocalId, UDInformation remoteHandlerInfo) {
        remoteHandlerInfos.put(remoteLocalId, remoteHandlerInfo);
    }

    protected UDInformation getRemoteHandlerInfo(int remoteLocalId) {
        return remoteHandlerInfos.get(remoteLocalId);
    }

    protected boolean hasRemoteHandlerInfo(int remoteLocalId) {
        return remoteHandlerInfos.containsKey(remoteLocalId);
    }

    protected short[] getRemoteLocalIds() {
        var n = remoteHandlerInfos.size();
        var remotes = remoteHandlerInfos.keySet().toArray();
        short[] lids = new short[n];

        for(int i = 0; i < lids.length; i++) {
            lids[i] =  (short) (int) remotes[i];
        }

        return lids;
    }

    protected void sendConnectionRequest(RCInformation localQP, short remoteLocalId) {
        executor.submit(new OutgoingMessageHandler(MessageType.CONNECTION_REQUEST, remoteLocalId, localQP.getPortNumber(), localQP.getLocalId(), localQP.getQueuePairNumber()));
        LOGGER.info("Initiate new reliable connection to {}", remoteLocalId);
    }

    protected void sendBufferInfo(BufferInformation bufferInformation, short localId, short remoteLocalId) {
        executor.submit(new OutgoingMessageHandler(MessageType.BUFFER_INFO, remoteLocalId, localId, bufferInformation.getAddress(), bufferInformation.getCapacity(), bufferInformation.getRemoteKey()));
        LOGGER.trace("Send buffer info to {}", remoteLocalId);
    }

    protected void sendDisconnect(short localId, short remoteLocalId) {
        executor.submit(new OutgoingMessageHandler(MessageType.DISCONNECT, remoteLocalId, localId));
        LOGGER.trace("Send disconnect to {}", remoteLocalId);
    }

    protected void sendBufferAck(short localId, short remoteLocalId) {
        executor.submit(new OutgoingMessageHandler(MessageType.BUFFER_ACK, remoteLocalId, localId));
        LOGGER.trace("Send Buffer ack to {}", remoteLocalId);
    }

    protected void sendMessage(MessageType msgType, UDInformation remoteInfo, long ... payload) {
        var sge = sendSGEProvider.getSGE();
        if(sge == null) {
            LOGGER.error("Cannot post another send request");
            return;
        }

        var msg = new LocalMessage(LocalBuffer.wrap(sge.getAddress(), sge.getLength()));
        msg.setMessageType(msgType);
        msg.setPayload(payload);


        var workRequest = buildSendWorkRequest(sge, remoteInfo, sendWrIdProvider.getAndIncrement() % MAX_SEND_WORK_REQUESTS);
        sendWorkRequests[(int) workRequest.getId()] = workRequest;

        postSend(workRequest);
    }

    protected void receiveMessage() {
        var sge = receiveSGEProvider.getSGE();
        if(sge == null) {
            LOGGER.error("Cannot post another receive request");
            return;
        }

        var workRequest = buildReceiveWorkRequest(sge, receiveWrIdProvider.getAndIncrement() % MAX_RECEIVE_WORK_REQUESTS);
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
        udCqpt.shutdown();

        boolean killed = false;

        while(!killed) {
            killed = udCqpt.isKilled();
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

                    executor.submit(new IncomingMessageHandler(message.getMessageType(), message.getPayload()));

                    receiveSGEProvider.returnSGE(sge);
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
        private final long[] payload;

        IncomingMessageHandler(MessageType msgType, long... payload) {
            this.msgType = msgType;
            this.payload = payload;
        }

        @Override
        public void run() {
            switch(msgType) {
                case CONNECTION_REQUEST:
                    handleConnectionRequest();
                    break;
                case BUFFER_INFO:
                    handleBufferInfo();
                    break;
                case DISCONNECT:
                    handleDisconnect();
                    break;
                case BUFFER_ACK:
                    handleBufferAck();
                    break;
                default:
                    LOGGER.info("Got message {}", payload);
                    break;
            }

        }

        private void handleConnectionRequest() {

            var remoteInfo = new RCInformation((byte) payload[0], (short) payload[1], (int) payload[2]);
            var remoteLocalId = remoteInfo.getLocalId();

            LOGGER.info("Got new connection request from {}", remoteInfo.getLocalId());

            boolean connected = false;

            while (!connected) {
                if (!dcm.connections.containsKey(remoteLocalId)) {
                    dcm.createConnection(remoteLocalId);
                }

                dcm.rwLocks.readLock(remoteLocalId);
                var connection = dcm.connections.get(remoteLocalId);

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

            sendBufferAck(dcm.getLocalId(), remoteLid);
        }

        private void handleBufferAck() {
            var remoteLid = (short) payload[0];

            dcm.localBufferHandler.setBufferInfoAck(remoteLid);
        }

        private void handleDisconnect() {
            // todo
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
            switch(msgType) {
                case BUFFER_INFO:
                    handleBufferInfo();
                    break;
                case CONNECTION_REQUEST:
                    handleConnectionRequest();
                    break;
                case DISCONNECT:
                    handleDisconnect();
                    break;
                case BUFFER_ACK:
                    handleBufferAck();
                    break;
                default:
                    LOGGER.info("Got message {}", payload);
                    break;
            }
        }

        private void handleBufferInfo() {

            dcm.localBufferHandler.setBufferInfoSent(remoteLocalId);

            boolean bufferAck = false;

            while (!bufferAck) {
                sendMessage(msgType, remoteHandlerInfos.get(remoteLocalId), payload);

                var start = System.currentTimeMillis();

                while (System.currentTimeMillis() - start < BUFFER_ACK_TIMEOUT) {
                    if(dcm.localBufferHandler.getBufferInfoStatus(remoteLocalId) == LocalBufferHandler.ACKNOWLEDGED) {
                        bufferAck = true;
                        break;
                    }
                }
            }

        }

        private void handleConnectionRequest() {
            sendMessage(msgType, remoteHandlerInfos.get(remoteLocalId), payload);
        }

        private void handleDisconnect() {
            sendMessage(msgType, remoteHandlerInfos.get(remoteLocalId), payload);
        }

        private void handleBufferAck() {
            sendMessage(msgType, remoteHandlerInfos.get(remoteLocalId), payload);
        }
    }
}
