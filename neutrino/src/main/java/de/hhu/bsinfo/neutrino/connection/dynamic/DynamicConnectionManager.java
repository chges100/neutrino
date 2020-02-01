package de.hhu.bsinfo.neutrino.connection.dynamic;

import de.hhu.bsinfo.neutrino.buffer.LocalBuffer;
import de.hhu.bsinfo.neutrino.connection.DeviceContext;
import de.hhu.bsinfo.neutrino.connection.ReliableConnection;
import de.hhu.bsinfo.neutrino.connection.UnreliableDatagram;
import de.hhu.bsinfo.neutrino.connection.message.LocalMessage;
import de.hhu.bsinfo.neutrino.connection.message.Message;
import de.hhu.bsinfo.neutrino.connection.message.MessageType;
import de.hhu.bsinfo.neutrino.connection.util.RCInformation;
import de.hhu.bsinfo.neutrino.connection.util.SGEProvider;
import de.hhu.bsinfo.neutrino.connection.util.UDInformation;
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
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.spi.AbstractResourceBundleProvider;

public class DynamicConnectionManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(DynamicConnectionManager.class);

    private final ArrayList<DeviceContext> deviceContexts;

    private final DynamicConnectionHandler dynamicConnectionHandler;
    private final UDInformation localUDInformation;

    private final HashMap<Short, UDInformation> remoteHandlerInfos;
    private final HashMap<Short, RCInformation> remoteConnectionInfos;
    private final HashMap<Short, ReliableConnection> connections;

    private final UDInformationPropagator udPropagator;
    private final UDInformationReceiver udReceiver;

    private final AtomicInteger idCounter = new AtomicInteger();
    private final int UDPPort;

    private final ThreadPoolExecutor executor;

    public DynamicConnectionManager(int port) throws IOException {
        LOGGER.info("Initialize dynamic connection handler");

        var deviceCnt = Context.getDeviceCount();
        deviceContexts = new ArrayList<>();
        remoteHandlerInfos = new HashMap<>();
        remoteConnectionInfos = new HashMap<>();
        connections = new HashMap<>();

        executor = (ThreadPoolExecutor) Executors.newCachedThreadPool();

        for (int i = 0; i < deviceCnt; i++) {
            var deviceContext = new DeviceContext(i);
            deviceContexts.add(deviceContext);
        }

        if(deviceContexts.isEmpty()) {
            throw new IOException("Could not initialize any Infiniband device");
        }

        UDPPort = port;

        LOGGER.info("Create UD to handle connection requests");
        dynamicConnectionHandler = new DynamicConnectionHandler(deviceContexts.get(0));
        dynamicConnectionHandler.init();
        LOGGER.info("UD data: {}", dynamicConnectionHandler);

        localUDInformation = new UDInformation((byte) 1, dynamicConnectionHandler.getPortAttributes().getLocalId(), dynamicConnectionHandler.getQueuePair().getQueuePairNumber(),
                dynamicConnectionHandler.getQueuePair().queryAttributes().getQkey());

        udPropagator = new UDInformationPropagator();
        udReceiver = new UDInformationReceiver();

        udReceiver.start();
        udPropagator.start();
    }

    public int provideConnectionId() {
        return idCounter.getAndIncrement();
    }

    public void shutdown() {
        LOGGER.info("Shutdown dynamic connection manager");

        udPropagator.shutdown();
        udReceiver.shutdown();

        dynamicConnectionHandler.close();

        printRemoteDCHInfos();
        printRemoteRCInfos();
    }

    public void printRemoteDCHInfos() {
        String out = "Print out remote dynamic connection handler information:\n";

        for(var remoteInfo : remoteHandlerInfos.values()) {
            out += remoteInfo;
            out += "\n";
        }

        LOGGER.info(out);
    }

    public void printRemoteRCInfos() {
        String out = "Print out reliable connection information:\n";

        for(var remoteInfo : connections.values()) {
            out += remoteInfo;
            out += "\n";
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

                if (!remoteHandlerInfos.containsKey(remoteInfo.getLocalId())) {
                    remoteHandlerInfos.put(remoteInfo.getLocalId(), remoteInfo);

                    LOGGER.info("Add new remote connection handler {}", remoteInfo);
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

            if(connections.containsKey(remoteInfo.getLocalId())) {
                var connection = connections.get(remoteInfo.getLocalId());

                var localQPInfo = new RCInformation((byte) 1, connection.getPortAttributes().getLocalId(), connection.getQueuePair().getQueuePairNumber());

                dynamicConnectionHandler.answerConnectionRequest(localQPInfo, remoteHandlerInfos.get(remoteInfo.getLocalId()));
            } else {
                try {
                    var connection = new ReliableConnection(deviceContexts.get(0));

                    var localQPInfo = new RCInformation((byte) 1, connection.getPortAttributes().getLocalId(), connection.getQueuePair().getQueuePairNumber());

                    dynamicConnectionHandler.answerConnectionRequest(localQPInfo, remoteHandlerInfos.get(remoteInfo.getLocalId()));

                    connections.put(remoteInfo.getLocalId(), connection);
                } catch (Exception e) {
                    LOGGER.error("Could not create connection to {}", remoteInfo);
                }



            }
        }
    }
}
