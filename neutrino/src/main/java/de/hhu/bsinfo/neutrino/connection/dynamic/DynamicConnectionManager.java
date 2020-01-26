package de.hhu.bsinfo.neutrino.connection.dynamic;

import de.hhu.bsinfo.neutrino.buffer.RegisteredBuffer;
import de.hhu.bsinfo.neutrino.connection.DeviceContext;
import de.hhu.bsinfo.neutrino.connection.UnreliableDatagram;
import de.hhu.bsinfo.neutrino.connection.util.UDInformation;
import de.hhu.bsinfo.neutrino.verbs.AccessFlag;
import de.hhu.bsinfo.neutrino.verbs.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

public class DynamicConnectionManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(DynamicConnectionManager.class);

    private final ArrayList<DeviceContext> deviceContexts;
    private final Deque<RegisteredBuffer> localBuffers;

    private final UnreliableDatagram dynamicConnectionHandler;
    private final UDInformation localUDInformation;

    private final HashMap<Short, UDInformation> remoteHandlerInfos;

    private final UDInformationPropagator udPropagator;
    private final UDInformationReceiver udReceiver;

    private final AtomicInteger idCounter = new AtomicInteger();
    private final int UDPPort;

    public DynamicConnectionManager(int port) throws IOException {
        LOGGER.info("Initialize dynamic connection handler");

        var deviceCnt = Context.getDeviceCount();
        deviceContexts = new ArrayList<>();
        localBuffers = new LinkedList<>();
        remoteHandlerInfos = new HashMap<>();

        for (int i = 0; i < deviceCnt; i++) {
            var deviceContext = new DeviceContext(i);
            deviceContexts.add(deviceContext);
        }

        if(deviceContexts.isEmpty()) {
            throw new IOException("Could not initialize any Infiniband device");
        }

        UDPPort = port;

        LOGGER.info("Create UD to handle connection requests");
        dynamicConnectionHandler = new UnreliableDatagram(deviceContexts.get(0));
        dynamicConnectionHandler.init();
        LOGGER.info("UD data: {}", dynamicConnectionHandler);

        localUDInformation = new UDInformation((byte) 1, dynamicConnectionHandler.getPortAttributes().getLocalId(), dynamicConnectionHandler.getQueuePair().getQueuePairNumber(),
                dynamicConnectionHandler.getQueuePair().queryAttributes().getQkey());

        udPropagator = new UDInformationPropagator();
        udReceiver = new UDInformationReceiver();

        udReceiver.start();
        udPropagator.start();
    }


    public RegisteredBuffer allocLocalBuffer(DeviceContext deviceContext, long size) {
        LOGGER.info("Allocate new memory region for device {} of size {}", deviceContext.getDeviceId(), size);

        var buffer = deviceContext.getProtectionDomain().allocateMemory(size, AccessFlag.LOCAL_WRITE, AccessFlag.REMOTE_READ, AccessFlag.REMOTE_WRITE, AccessFlag.MW_BIND);
        localBuffers.add(buffer);

        return buffer;
    }

    public RegisteredBuffer allocLocalBuffer(int deviceId, long size) {
        return allocLocalBuffer(deviceContexts.get(deviceId), size);
    }

    public void freeLocalBuffer(RegisteredBuffer buffer) {
        LOGGER.info("Free memory region");
        localBuffers.remove(buffer);
        buffer.close();
    }


    public int provideConnectionId() {
        return idCounter.getAndIncrement();
    }

    public void shutdown() {
        LOGGER.info("Shutdown dynamic connection manager");

        udPropagator.shutdown();
        udReceiver.shutdown();

        dynamicConnectionHandler.close();
    }

    public void printRemoteInfos() {
        LOGGER.info("Print out remote handler information:");
        for(var remoteInfo : remoteHandlerInfos.values()) {
            LOGGER.info("{}", remoteInfo);
        }
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
}
