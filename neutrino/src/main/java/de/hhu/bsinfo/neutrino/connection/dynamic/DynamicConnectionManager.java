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
import de.hhu.bsinfo.neutrino.connection.util.RCInformation;
import de.hhu.bsinfo.neutrino.connection.util.SGEProvider;
import de.hhu.bsinfo.neutrino.connection.util.UDInformation;
import de.hhu.bsinfo.neutrino.data.NativeString;
import de.hhu.bsinfo.neutrino.util.NativeObjectRegistry;
import de.hhu.bsinfo.neutrino.verbs.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.StampedLock;

public class DynamicConnectionManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(DynamicConnectionManager.class);

    private static final long BUFFER_SIZE = 1024*1024;
    private static final long EXECUTE_POLL_TIME = 1000;
    private static final long IDX_POLL_TIME = 1000;
    private static final int LOCAL_BUFFER_READ = 19;
    private static final int MAX_CONNECTONS = 3;
    private static final int NULL_INDEX = Integer.MAX_VALUE;
    private static final short LID_MAX = Short.MAX_VALUE;

    private final short localId;

    private final ArrayList<DeviceContext> deviceContexts;

    private final DynamicConnectionHandler dynamicConnectionHandler;
    private final UDInformation localUDInformation;

    private final HashMap<Short, UDInformation> remoteHandlerInfos;

    private final AtomicIntegerArray lidToIndex;

    private final ReliableConnection connections[];
    private final StampedLock rwLocks[];

    private final ConcurrentLinkedQueue<Integer> lru;
    private final BufferInformation remoteBuffers[];
    private final RegisteredBuffer localBuffers[];


    private final UDInformationPropagator udPropagator;
    private final UDInformationReceiver udReceiver;

    private final int UDPPort;

    private final ThreadPoolExecutor executor;

    public DynamicConnectionManager(int port) throws IOException {
        LOGGER.info("Initialize dynamic connection handler");

        deviceContexts = new ArrayList<>();
        remoteHandlerInfos = new HashMap<>();

        lidToIndex = new AtomicIntegerArray(LID_MAX);
        for(int i = 0; i < LID_MAX; i++) {
            lidToIndex.set(i, NULL_INDEX);
        }

        remoteBuffers = new BufferInformation[LID_MAX];
        localBuffers = new RegisteredBuffer[LID_MAX];

        executor = (ThreadPoolExecutor) Executors.newCachedThreadPool();

        var deviceCnt = Context.getDeviceCount();

        for (int i = 0; i < deviceCnt; i++) {
            var deviceContext = new DeviceContext(i);
            deviceContexts.add(deviceContext);
        }

        localId = deviceContexts.get(0).getContext().queryPort().getLocalId();

        if(deviceContexts.isEmpty()) {
            throw new IOException("Could not initialize any Infiniband device");
        }

        connections = new ReliableConnection[MAX_CONNECTONS];
        rwLocks = new StampedLock[MAX_CONNECTONS];
        lru = new ConcurrentLinkedQueue<>();

        for(int i = 0; i < MAX_CONNECTONS; i++) {
            connections[i] = new ReliableConnection(deviceContexts.get(0));
            connections[i].init();

            rwLocks[i] = new StampedLock();

            lru.offer(i);
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
    }

    public void remoteWriteToAll(RegisteredBuffer data, long offset, long length) {
        for(var remoteLocalId : remoteHandlerInfos.keySet()){
            remoteWrite(data, offset, length, remoteLocalId);
        }
    }

    public void remoteReadFromAll(RegisteredBuffer data, long offset, long length) {
        for(var remoteLocalId : remoteHandlerInfos.keySet()){
            remoteRead(data, offset, length, remoteLocalId);
        }
    }

    public void remoteWrite(RegisteredBuffer data, long offset, long length, short remoteLocalId) {
        remoteExecute(SendWorkRequest.OpCode.RDMA_WRITE, data, offset, length, remoteLocalId);
    }

    public void remoteRead(RegisteredBuffer data, long offset, long length, short remoteLocalId) {
        remoteExecute(SendWorkRequest.OpCode.RDMA_READ, data, offset, length, remoteLocalId);
    }

    private void remoteExecute(SendWorkRequest.OpCode opCode, RegisteredBuffer data, long offset, long length, short remoteLocalId) {
        long stamp = 0;
        while (stamp == 0) {
            try {
                stamp = rwLocks[lidToIndex.get(remoteLocalId)].tryReadLock();
            } catch (IndexOutOfBoundsException e) {
                createConnection(remoteLocalId, false);
            }
        }

        int idx = lidToIndex.get(remoteLocalId);

        while (remoteBuffers[localId] == null || !connections[idx].isConnected()) {
            LockSupport.parkNanos(EXECUTE_POLL_TIME);
        }

        connections[idx].execute(data, opCode, offset, length, remoteBuffers[remoteLocalId].getAddress(), remoteBuffers[remoteLocalId].getRemoteKey(), 0);

        rwLocks[idx].unlockRead(stamp);
    }

    private void createConnection(short remoteLocalId, boolean answerRequest) {

        int idx = NULL_INDEX;

        // if no ids are available poll
        boolean gotID;
        do{
            try {
                idx = lru.poll();
                gotID = true;
            } catch (NullPointerException e) {
                gotID = false;
                LockSupport.parkNanos(IDX_POLL_TIME);
            }
        } while (!gotID);

        LOGGER.debug("LRU gave index {}", idx);


        try {
            long stamp = rwLocks[idx].writeLock();

            short oldRemoteLid = connections[idx].disconnect();

            if(oldRemoteLid < LID_MAX) {
                lidToIndex.set(oldRemoteLid, NULL_INDEX);
                dynamicConnectionHandler.sendMessage(MessageType.DISCONNECT, localId + "", remoteHandlerInfos.get(oldRemoteLid));
            }

            lidToIndex.set(remoteLocalId, idx);
            var localQPInfo = new RCInformation((byte) 1, connections[idx].getPortAttributes().getLocalId(), connections[idx].getQueuePair().getQueuePairNumber());

            if(answerRequest) {
                dynamicConnectionHandler.answerConnectionRequest(localQPInfo, remoteHandlerInfos.get(remoteLocalId));
                LOGGER.info("Answer new reliable connection request from {}", remoteLocalId);
            } else {
                dynamicConnectionHandler.sendConnectionRequest(localQPInfo, remoteLocalId);
                LOGGER.info("Initiate new reliable connection to {}", remoteLocalId);
            }

            if (localBuffers[remoteLocalId] == null) {
                var buffer = deviceContexts.get(0).allocRegisteredBuffer(BUFFER_SIZE);
                var bufferInfo = new BufferInformation(buffer);

                localBuffers[remoteLocalId] = buffer;

                dynamicConnectionHandler.sendBufferInfo(bufferInfo, connections[idx].getPortAttributes().getLocalId(), remoteLocalId);
            }

        } catch (Exception e) {
            LOGGER.error("Could not create connection to {}\n {}", remoteLocalId, e);
            e.printStackTrace();
        } finally {
            if(rwLocks[idx].isWriteLocked()) {
                rwLocks[idx].tryUnlockWrite();
            }

            //lru.offer(idx);
        }
    }

    public RegisteredBuffer allocRegisteredBuffer(int deviceId, long size) {
        return deviceContexts.get(deviceId).allocRegisteredBuffer(size);
    }

    public void shutdown() {
        LOGGER.info("Shutdown dynamic connection manager");

        udPropagator.shutdown();
        udReceiver.shutdown();

        executor.shutdown();
        dynamicConnectionHandler.close();

        try {
            for(var connection : connections) {
                connection.disconnect();
            }
        } catch (Exception e) {
            LOGGER.error("Could not disconnect all connections: {}", e);
        }


        printRemoteDCHInfos();
        printRCInfos();
        printRemoteBufferInfos();
        printLocalBufferInfos();
    }

    public short getLocalId() {
        return localId;
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

        for(var connection : connections) {
            out += connection;
            out += "\n";
            out += "connected: " + connection.isConnected();
            out += "\n";
        }

        LOGGER.info(out);

    }

    public void printRemoteBufferInfos() {
        String out = "Print out remote buffer information:\n";

        for(int i = 0; i < remoteBuffers.length; i++) {
            if(remoteBuffers[i] != null) {
                out += "LocalId " + i + " : ";
                out += remoteBuffers[i];
                out += "\n";
            }

        }

        LOGGER.info(out);

    }

    public void printLocalBufferInfos() {
        String out = "Content of local connection RDMA buffers:\n";

        for(int i = 0; i < localBuffers.length; i++) {

            if(localBuffers[i] != null) {
                out += "Buffer for remote " + i +  " with address " + localBuffers[i].getHandle() + ": ";
                var tmp = new NativeString(localBuffers[i], 0, LOCAL_BUFFER_READ);
                out += tmp.get() + "\n";
            }
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

                if (!remoteHandlerInfos.containsKey(remoteInfo.getLocalId()) && remoteInfo.getLocalId() != localId) {
                    remoteHandlerInfos.put(remoteInfo.getLocalId(), remoteInfo);

                    LOGGER.info("UDP: Add new remote connection handler {}", remoteInfo);
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
        private final int maxSendWorkRequests = 250;
        private final int maxReceiveWorkRequests = 250;

        private final SendWorkRequest sendWorkRequests[];
        private final ReceiveWorkRequest receiveWorkRequests[];

        private final SGEProvider sendSGEProvider;
        private final SGEProvider receiveSGEProvider;

        private final CompletionQueuePollThread cqpt;

        public DynamicConnectionHandler(DeviceContext deviceContext) throws IOException  {

            super(deviceContext);

            LOGGER.info("Set up dynamic connection handler");

            receiveQueueFillCount = new AtomicInteger(0);

            sendWorkRequests = new SendWorkRequest[maxSendWorkRequests];
            receiveWorkRequests = new ReceiveWorkRequest[maxReceiveWorkRequests];

            sendSGEProvider = new SGEProvider(getDeviceContext(), maxSendWorkRequests, Message.getSize());
            receiveSGEProvider = new SGEProvider(getDeviceContext(), maxReceiveWorkRequests, Message.getSize() + UD_Receive_Offset);

            cqpt = new CompletionQueuePollThread();
            cqpt.start();
        }

        public void answerConnectionRequest(RCInformation localQP, UDInformation remoteInfo) {
            sendMessage(MessageType.CONNECTION_ACK, localQP.getPortNumber() + ":" + localQP.getLocalId() + ":" + localQP.getQueuePairNumber(), remoteInfo);
        }

        public void sendConnectionRequest(RCInformation localQP, short remoteLocalId) {
            sendMessage(MessageType.CONNECTION_REQUEST, localQP.getPortNumber() + ":" + localQP.getLocalId() + ":" + localQP.getQueuePairNumber(), remoteHandlerInfos.get(remoteLocalId));
        }

        public void sendBufferInfo(BufferInformation bufferInformation, short localId, short remoteLocalId) {
            sendMessage(MessageType.BUFFER_INFO, localId + ":" + bufferInformation.getAddress() + ":" + bufferInformation.getCapacity() + ":" + bufferInformation.getRemoteKey(), remoteHandlerInfos.get(remoteLocalId));
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
            cqpt.shutdown();
            queuePair.close();
        }

        private class CompletionQueuePollThread extends Thread {

            private boolean isRunning = true;
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

            }

            public void shutdown() {
                isRunning = false;
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
                case CONNECTION_ACK:
                    handleConnectionAck();
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

            LOGGER.info("Got new connection request from {}", remoteInfo);

            RCInformation localQPInfo;

            if(lidToIndex.get(remoteInfo.getLocalId()) != NULL_INDEX) {
                var idx = lidToIndex.get(remoteInfo.getLocalId());

                localQPInfo = new RCInformation((byte) 1, connections[idx].getPortAttributes().getLocalId(), connections[idx].getQueuePair().getQueuePairNumber());

                dynamicConnectionHandler.answerConnectionRequest(localQPInfo, remoteHandlerInfos.get(remoteInfo.getLocalId()));

            } else {
                createConnection(remoteInfo.getLocalId(), true);
            }
        }

        private void handleConnectionAck() {
            var split = payload.split(":");
            var remoteInfo = new RCInformation(Byte.parseByte(split[0]), Short.parseShort(split[1]), Integer.parseInt(split[2]));

            LOGGER.info("Got new connection ack from {}", remoteInfo);

            var idx = lidToIndex.get(remoteInfo.getLocalId());
            long stamp = rwLocks[idx].readLock();

            try {
                connections[idx].connect(remoteInfo);
                lru.offer(idx);
            } catch (Exception e) {
                LOGGER.error("Could not connect to {}", remoteInfo);
            } finally {
                rwLocks[idx].unlockRead(stamp);
            }

        }

        private void handleBufferInfo() {
            var split = payload.split(":");
            var bufferInfo = new BufferInformation(Long.parseLong(split[1]), Long.parseLong(split[2]), Integer.parseInt(split[3]));

            LOGGER.info("Received new remote buffer information: {}", bufferInfo);

            remoteBuffers[Short.parseShort(split[0])] = bufferInfo;
        }

        private void handleDisconnect() {

            // TODO: switch to lock-based
            var remoteLocalId = Short.parseShort(payload);

            var idx = lidToIndex.getAndSet(remoteLocalId, LID_MAX);
            try {
                connections[idx].disconnect();
            } catch (Exception e) {
                LOGGER.error("Could not disconnect connection {}", idx);
            }

            lru.remove(idx);
            lru.add(idx);
        }
    }
}