package de.hhu.bsinfo.neutrino.connection.dynamic;

import de.hhu.bsinfo.neutrino.buffer.BufferInformation;
import de.hhu.bsinfo.neutrino.buffer.LocalBuffer;
import de.hhu.bsinfo.neutrino.buffer.RegisteredBuffer;
import de.hhu.bsinfo.neutrino.connection.DeviceContext;
import de.hhu.bsinfo.neutrino.connection.ReliableConnection;
import de.hhu.bsinfo.neutrino.connection.UnreliableDatagram;
import de.hhu.bsinfo.neutrino.connection.message.LocalMessage;
import de.hhu.bsinfo.neutrino.connection.message.Message;
import de.hhu.bsinfo.neutrino.connection.message.MessageType;
import de.hhu.bsinfo.neutrino.connection.util.AtomicReadWriteLockArray;
import de.hhu.bsinfo.neutrino.connection.util.RCInformation;
import de.hhu.bsinfo.neutrino.connection.util.SGEProvider;
import de.hhu.bsinfo.neutrino.connection.util.UDInformation;
import de.hhu.bsinfo.neutrino.data.NativeString;
import de.hhu.bsinfo.neutrino.util.NativeObjectRegistry;
import de.hhu.bsinfo.neutrino.verbs.*;
import org.agrona.collections.Int2ObjectHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.StampedLock;

public class DynamicConnectionManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(DynamicConnectionManager.class);

    private static final long BUFFER_SIZE = 1024*1024;
    private static final int RC_COMPLETION_QUEUE_POLL_BATCH_SIZE = 200;

    private static final int LOCAL_BUFFER_READ = 19;
    private static final short MAX_LID = Short.MAX_VALUE;

    private final short localId;

    private final ArrayList<DeviceContext> deviceContexts;

    private final DynamicConnectionHandler dynamicConnectionHandler;
    private final UDInformation localUDInformation;

    private final Int2ObjectHashMap<UDInformation> remoteHandlerInfos;
    private final Int2ObjectHashMap<ReliableConnection> connections;
    private final Int2ObjectHashMap<BufferInformation> remoteBuffers;
    private final Int2ObjectHashMap<RegisteredBuffer> localBuffers;

    private final AtomicReadWriteLockArray rwLocks;

    private final UDInformationPropagator udPropagator;
    private final UDInformationReceiver udReceiver;

    private final RCCompletionQueuePollThread rcCqpt;

    private final int UDPPort;

    private final ThreadPoolExecutor executor;

    public DynamicConnectionManager(int port) throws IOException {
        LOGGER.info("Initialize dynamic connection handler");

        deviceContexts = new ArrayList<>();
        remoteHandlerInfos = new Int2ObjectHashMap<>();

        remoteBuffers = new Int2ObjectHashMap<>();
        localBuffers = new Int2ObjectHashMap<>();
        connections = new Int2ObjectHashMap<>();

        rwLocks = new AtomicReadWriteLockArray(MAX_LID);

        executor = (ThreadPoolExecutor) Executors.newCachedThreadPool();

        var deviceCnt = Context.getDeviceCount();

        for (int i = 0; i < deviceCnt; i++) {
            var deviceContext = new DeviceContext(i);
            deviceContexts.add(deviceContext);
        }

        localId = deviceContexts.get(0).getContext().queryPort().getLocalId();
        LOGGER.debug("Local Id is {}", localId);

        if(deviceContexts.isEmpty()) {
            throw new IOException("Could not initialize any Infiniband device");
        }

        UDPPort = port;

        LOGGER.trace("Create UD to handle connection requests");
        dynamicConnectionHandler = new DynamicConnectionHandler(deviceContexts.get(0));
        dynamicConnectionHandler.init();
        LOGGER.info("Data of UD: {}", dynamicConnectionHandler);

        localUDInformation = new UDInformation((byte) 1, dynamicConnectionHandler.getPortAttributes().getLocalId(), dynamicConnectionHandler.getQueuePair().getQueuePairNumber(),
                dynamicConnectionHandler.getQueuePair().queryAttributes().getQkey());

        udPropagator = new UDInformationPropagator();
        udReceiver = new UDInformationReceiver();

        udReceiver.start();
        udPropagator.start();

        rcCqpt = new RCCompletionQueuePollThread(RC_COMPLETION_QUEUE_POLL_BATCH_SIZE);
        rcCqpt.start();
    }

    public void remoteWriteToAll(RegisteredBuffer data, long offset, long length) {
        for(var remoteLocalId : remoteHandlerInfos.keySet()){
            remoteWrite(data, offset, length, remoteLocalId.shortValue());
        }
    }

    public void remoteReadFromAll(RegisteredBuffer data, long offset, long length) {
        for(var remoteLocalId : remoteHandlerInfos.keySet()){
            remoteRead(data, offset, length, remoteLocalId.shortValue());
        }
    }

    public void remoteWrite(RegisteredBuffer data, long offset, long length, short remoteLocalId) {
        remoteExecute(SendWorkRequest.OpCode.RDMA_WRITE, data, offset, length, remoteLocalId);
    }

    public void remoteRead(RegisteredBuffer data, long offset, long length, short remoteLocalId) {
        remoteExecute(SendWorkRequest.OpCode.RDMA_READ, data, offset, length, remoteLocalId);
    }

    private void remoteExecute(SendWorkRequest.OpCode opCode, RegisteredBuffer data, long offset, long length, short remoteLocalId) {
        boolean connected = false;

        while (!connected) {
            if(!connections.containsKey(remoteLocalId)) {
                createConnection(remoteLocalId);
            }

            if(connections.get(remoteLocalId).isConnected()) {
                connected = true;
            }
        }

        connections.get(remoteLocalId).execute(data, opCode, offset, length, remoteBuffers.get(remoteLocalId).getAddress(), remoteBuffers.get(remoteLocalId).getRemoteKey(), 0);

    }

    private void createConnection(short remoteLocalId) {
        try {
            var connection = new ReliableConnection(deviceContexts.get(0));
            connection.init();

            var localQP = new RCInformation(connection);
            dynamicConnectionHandler.sendConnectionRequest(localQP, remoteLocalId);

            connections.put(remoteLocalId, connection);
        } catch (IOException e) {
            LOGGER.error("Could not create connection to {}", remoteLocalId);
        }

    }

    public RegisteredBuffer allocRegisteredBuffer(int deviceId, long size) {
        return deviceContexts.get(deviceId).allocRegisteredBuffer(size);
    }

    public void shutdown() {
        LOGGER.info("Shutdown dynamic connection manager");

        udPropagator.shutdown();
        udReceiver.shutdown();

        executor.shutdownNow();

        rcCqpt.shutdown();

        try {
            executor.awaitTermination(500, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            LOGGER.info("Thread Pool termination not yet finished - continue shutdown");
        }

        dynamicConnectionHandler.close();


        for(var connection : connections.values()) {
            if(connection.isConnected()) {
                var t = new Thread(() -> {
                    try {
                        connection.disconnect();
                    } catch (IOException e) {
                        LOGGER.debug("Error disconnecting connection {}: {}", connection.getId(), e);
                    }
                });
            }
        }

        printLocalBufferInfos();
    }

    public short getLocalId() {
        return localId;
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

    public void printRemoteDCHInfos() {
        String out = "Print out remote dynamic connection handler information:\n";

        for(var remoteInfo : remoteHandlerInfos.values()) {
            out += remoteInfo;
            out += "\n";
        }

        LOGGER.info(out);
    }

    public void printRCInfos() {
        String out = "Print out reliable connection information:\n";

        for(var connection : connections.values()) {
            out += connection;
            out += "\n";
            out += "connected: " + connection.isConnected();
            out += "\n";
        }

        LOGGER.info(out);

    }

    public void printRemoteBufferInfos() {
        String out = "Print out remote buffer information:\n";

        for(var remoteBuffer : remoteBuffers.values()) {
            out += remoteBuffer;
            out += "\n";
        }

        LOGGER.info(out);

    }

    public void printLocalBufferInfos() {
        String out = "Content of local connection RDMA buffers:\n";

        for(var localBuffer : localBuffers.values()) {

            out += "Buffer for remote with address " + localBuffer.getHandle() + ": ";
            var tmp = new NativeString(localBuffer, 0, LOCAL_BUFFER_READ);
            out += tmp.get() + "\n";

        }

        LOGGER.info(out);
    }

    private class UDInformationReceiver extends Thread {

        private final DatagramSocket datagramSocket;
        private final DatagramPacket datagram;

        private boolean isRunning = true;

        private final int timeout = 500;

        UDInformationReceiver() throws IOException {
            setName("UDInformationReceiver");

            datagramSocket = new DatagramSocket(UDPPort);
            datagramSocket.setSoTimeout(timeout);

            var buffer = ByteBuffer.allocate(UDInformation.SIZE).array();
            datagram = new DatagramPacket(buffer, buffer.length);
        }

        @Override
        public void run() {
            while (isRunning) {
                try {
                    datagramSocket.receive(datagram);
                } catch (Exception e) {

                }

                var data = ByteBuffer.wrap(datagram.getData());
                var remoteInfo = new UDInformation(data);

                var remoteLocalId = remoteInfo.getLocalId();

                if (!remoteHandlerInfos.containsKey(remoteLocalId) && remoteLocalId != localId) {
                    // put remote handler info into map
                    remoteHandlerInfos.put(remoteInfo.getLocalId(), remoteInfo);
                    // create buffer for RDMA test
                    if (!localBuffers.containsKey(remoteLocalId)) {
                        var buffer = deviceContexts.get(0).allocRegisteredBuffer(BUFFER_SIZE);
                        var bufferInfo = new BufferInformation(buffer);

                        localBuffers.put(remoteLocalId, buffer);
                        buffer.clear();

                        dynamicConnectionHandler.sendBufferInfo(bufferInfo, localId, remoteLocalId);
                    }

                    LOGGER.trace("UDP: Add new remote connection handler {}", remoteInfo);
                }
            }
        }

        public void shutdown() {
            isRunning = false;
        }
    }

    private class UDInformationPropagator extends Thread {

        private final DatagramSocket datagramSocket;
        private final DatagramPacket datagram;

        private final long parkNanos = 1000000000;

        private boolean isRunning = true;

        UDInformationPropagator() throws IOException {
            setName("UDInformationPropagator");

            datagramSocket = new DatagramSocket();
            datagramSocket.setBroadcast(true);

            var buffer = ByteBuffer.allocate(UDInformation.SIZE)
                    .put(localUDInformation.getPortNumber())
                    .putShort(localUDInformation.getLocalId())
                    .putInt(localUDInformation.getQueuePairNumber())
                    .putInt(localUDInformation.getQueuePairKey())
                    .array();

            datagram = new DatagramPacket(buffer, buffer.length, InetAddress.getByName("255.255.255.255"), UDPPort);
        }

        @Override
        public void run() {
            while(isRunning) {
                try {
                    datagramSocket.send(datagram);
                } catch (Exception e) {
                    LOGGER.error("Could not broadcast UD information");
                }

                LockSupport.parkNanos(parkNanos);
            }
        }

        public void shutdown() {
            isRunning = false;
        }
    }

    private final class DynamicConnectionHandler extends UnreliableDatagram {

        private AtomicInteger receiveQueueFillCount;
        private final int maxSendWorkRequests = 500;
        private final int maxReceiveWorkRequests = 500;

        private final SendWorkRequest sendWorkRequests[];
        private final ReceiveWorkRequest receiveWorkRequests[];


        private final SGEProvider sendSGEProvider;
        private final SGEProvider receiveSGEProvider;

        private final UDCompletionQueuePollThread udCqpt;

        public DynamicConnectionHandler(DeviceContext deviceContext) throws IOException  {

            super(deviceContext);

            LOGGER.info("Set up dynamic connection handler");

            receiveQueueFillCount = new AtomicInteger(0);

            sendWorkRequests = new SendWorkRequest[maxSendWorkRequests];
            receiveWorkRequests = new ReceiveWorkRequest[maxReceiveWorkRequests];

            sendSGEProvider = new SGEProvider(getDeviceContext(), maxSendWorkRequests, Message.getSize());
            receiveSGEProvider = new SGEProvider(getDeviceContext(), maxReceiveWorkRequests, Message.getSize() + UD_Receive_Offset);

            udCqpt = new UDCompletionQueuePollThread();
            udCqpt.start();
        }

        public void answerConnectionRequest(RCInformation localQP, UDInformation remoteInfo) {
            sendMessage(MessageType.CONNECTION_ACK, localQP.getPortNumber() + ":" + localQP.getLocalId() + ":" + localQP.getQueuePairNumber(), remoteInfo);
            LOGGER.info("Answer new reliable connection request from {}", remoteInfo.getLocalId());
        }

        public void sendConnectionRequest(RCInformation localQP, short remoteLocalId) {
            sendMessage(MessageType.CONNECTION_REQUEST, localQP.getPortNumber() + ":" + localQP.getLocalId() + ":" + localQP.getQueuePairNumber(), remoteHandlerInfos.get(remoteLocalId));
            LOGGER.info("Initiate new reliable connection to {}", remoteLocalId);
        }

        public void sendBufferInfo(BufferInformation bufferInformation, short localId, short remoteLocalId) {
            sendMessage(MessageType.BUFFER_INFO, localId + ":" + bufferInformation.getAddress() + ":" + bufferInformation.getCapacity() + ":" + bufferInformation.getRemoteKey(), remoteHandlerInfos.get(remoteLocalId));
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


            var workRequest = buildSendWorkRequest(sge, remoteInfo, sendWrIdProvider.getAndIncrement() % maxSendWorkRequests);
            sendWorkRequests[(int) workRequest.getId()] = workRequest;

            postSend(workRequest);
        }

        public void receiveMessage() {
            var sge = receiveSGEProvider.getSGE();
            if(sge == null) {
                LOGGER.error("Cannot post another receive request");
                return;
            }

            var workRequest = buildReceiveWorkRequest(sge, receiveWrIdProvider.getAndIncrement() % maxReceiveWorkRequests);
            receiveWorkRequests[(int) workRequest.getId()] = workRequest;

            postReceive(workRequest);
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

            if(!connections.containsKey(remoteLocalId)) {
                createConnection(remoteLocalId);
            }

            try {
                connections.get(remoteLocalId).connect(remoteInfo);
            } catch (IOException e) {
                LOGGER.debug("Could not connect to remote {}\n{}", remoteLocalId, e);
            }

        }

        private void handleBufferInfo() {
            var split = payload.split(":");
            var bufferInfo = new BufferInformation(Long.parseLong(split[1]), Long.parseLong(split[2]), Integer.parseInt(split[3]));
            var remoteLid = Short.parseShort(split[0]);

            LOGGER.info("Received new remote buffer information from {}: {}", remoteLid, bufferInfo);

            remoteBuffers.put(remoteLid, bufferInfo);
        }

        private void handleDisconnect() {

        }
    }

    private class RCCompletionQueuePollThread extends Thread {
        private boolean isRunning = true;
        private final int batchSize;

        public RCCompletionQueuePollThread(int batchSize) {
            this.batchSize = batchSize;
        }

        @Override
        public void run() {
            while(isRunning) {
                for(var connection : connections.values()) {

                    if(connection.isConnected()) {
                        try {
                            connection.pollSend(batchSize);
                        } catch (IOException e) {
                            LOGGER.error(e.toString());
                        }
                    }
                }
            }
        }

        public void shutdown() {
            isRunning = false;
        }
    }
}