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

public final class DynamicConnectionHandler extends UnreliableDatagram {
    private static final Logger LOGGER = LoggerFactory.getLogger(DynamicConnectionHandler.class);

    private static final int MAX_SEND_WORK_REQUESTS = 500;
    private static final int MAX_RECEIVE_WORK_REQUESTS = 500;

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

    public DynamicConnectionHandler(DynamicConnectionManager dcm, DeviceContext deviceContext) throws IOException {

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

    public UDInformation getLocalUDInformation() {
        return localUDInformation;
    }

    public void registerRemoteConnectionHandler(int remoteLocalId, UDInformation remoteHandlerInfo) {
        remoteHandlerInfos.put(remoteLocalId, remoteHandlerInfo);
    }

    public UDInformation getRemoteHandlerInfo(int remoteLocalId) {
        return remoteHandlerInfos.get(remoteLocalId);
    }

    public boolean hasRemoteHandlerInfo(int remoteLocalId) {
        return remoteHandlerInfos.containsKey(remoteLocalId);
    }

    public short[] getRemoteLocalIds() {
        var n = remoteHandlerInfos.size();
        var remotes = remoteHandlerInfos.keySet().toArray();
        short[] lids = new short[n];

        for(int i = 0; i < lids.length; i++) {
            lids[i] =  (short) (int) remotes[i];
        }

        return lids;
    }

    public void sendConnectionRequest(RCInformation localQP, short remoteLocalId) {
        sendMessage(MessageType.CONNECTION_REQUEST, localQP.getPortNumber() + ":" + localQP.getLocalId() + ":" + localQP.getQueuePairNumber(), remoteHandlerInfos.get(remoteLocalId));
        LOGGER.info("Initiate new reliable connection to {}", remoteLocalId);
    }

    public void sendBufferInfo(BufferInformation bufferInformation, short localId, short remoteLocalId) {
        sendMessage(MessageType.BUFFER_INFO, localId + ":" + bufferInformation.getAddress() + ":" + bufferInformation.getCapacity() + ":" + bufferInformation.getRemoteKey(), remoteHandlerInfos.get(remoteLocalId));
        LOGGER.trace("Send buffer info to {}", remoteLocalId);
    }

    public void sendDisconnect(short localId, short remoteLocalId) {
        sendMessage(MessageType.DISCONNECT, localId + "", remoteHandlerInfos.get(remoteLocalId));
        LOGGER.debug("Send disconnect to {}", remoteLocalId);
    }

    public void sendMessage(MessageType msgType, String payload, UDInformation remoteInfo) {
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

    public void receiveMessage() {
        var sge = receiveSGEProvider.getSGE();
        if(sge == null) {
            LOGGER.error("Cannot post another receive request");
            return;
        }

        var workRequest = buildReceiveWorkRequest(sge, receiveWrIdProvider.getAndIncrement() % MAX_RECEIVE_WORK_REQUESTS);
        receiveWorkRequests[(int) workRequest.getId()] = workRequest;

        postReceive(workRequest);
    }

    public void shutdown() {
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
        private final String payload;

        IncomingMessageHandler(MessageType msgType, String payload) {
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
                default:
                    LOGGER.info("Got message {}", payload);
                    break;
            }

        }

        private void handleConnectionRequest() {

            var split = payload.split(":");
            var remoteInfo = new RCInformation(Byte.parseByte(split[0]), Short.parseShort(split[1]), Integer.parseInt(split[2]));
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
            var split = payload.split(":");
            var bufferInfo = new BufferInformation(Long.parseLong(split[1]), Long.parseLong(split[2]), Integer.parseInt(split[3]));
            var remoteLid = Short.parseShort(split[0]);

            LOGGER.trace("Received new remote buffer information from {}: {}", remoteLid, bufferInfo);

            dcm.remoteBuffers.put(remoteLid, bufferInfo);
        }

        private void handleDisconnect() {
            // todo
        }
    }
}
