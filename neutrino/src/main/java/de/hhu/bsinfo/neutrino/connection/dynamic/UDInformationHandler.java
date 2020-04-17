package de.hhu.bsinfo.neutrino.connection.dynamic;

import de.hhu.bsinfo.neutrino.buffer.BufferInformation;
import de.hhu.bsinfo.neutrino.connection.util.UDInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.locks.LockSupport;

public class UDInformationHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(UDInformationHandler.class);

    private final DynamicConnectionManager dcm;
    private final DatagramSocket datagramSocket;


    private final UDInformationReceiver udReceiver;
    private final UDInformationPropagator udPropagator;

    private boolean isRunning  = true;

    private final int timeout = 500;
    private final int port;

    private final short localId;

    protected UDInformationHandler(DynamicConnectionManager dcm, int port) throws IOException {
        this.dcm = dcm;
        this.port = port;
        this.localId = dcm.getLocalId();

        datagramSocket = new DatagramSocket(port);
        datagramSocket.setSoTimeout(timeout);
        datagramSocket.setBroadcast(true);

        udReceiver = new UDInformationReceiver();
        udPropagator = new UDInformationPropagator();
    }

    protected void start() {
        udReceiver.start();
        udPropagator.start();
    }

    protected void shutdown() {
        isRunning = false;
    }

    private class UDInformationReceiver extends Thread {

        private final DatagramPacket datagram;

        UDInformationReceiver() throws IOException {
            setName("UDInformationReceiver");

            var buffer = ByteBuffer.allocate(UDInformation.SIZE).array();
            datagram = new DatagramPacket(buffer, buffer.length);
        }

        @Override
        public void run() {
            while (isRunning) {
                try {
                    datagramSocket.receive(datagram);
                } catch (Exception e) {
                    LOGGER.trace("Error receiving UDP datagram");
                }

                var data = ByteBuffer.wrap(datagram.getData());
                var remoteInfo = new UDInformation(data);

                var remoteLocalId = remoteInfo.getLocalId();

                if (!dcm.dch.hasRemoteHandlerInfo(remoteLocalId) && remoteLocalId != localId) {
                    // put remote handler info into map
                    dcm.dch.registerRemoteConnectionHandler(remoteLocalId, remoteInfo);
                    // create buffer for RDMA test
                    if (!dcm.localBufferHandler.hasBuffer(remoteLocalId)) {
                        dcm.localBufferHandler.createAndRegisterBuffer(remoteLocalId);
                    }

                    LOGGER.info("UDP: Add new remote connection handler {}", remoteInfo);
                }
            }
        }
    }

    private class UDInformationPropagator extends Thread {
        private final DatagramPacket datagram;

        private final long sleepTime = 1000;

        UDInformationPropagator() throws IOException {
            setName("UDInformationPropagator");

            var localUDInformation = dcm.dch.getLocalUDInformation();

            var buffer = ByteBuffer.allocate(UDInformation.SIZE)
                    .put(localUDInformation.getPortNumber())
                    .putShort(localUDInformation.getLocalId())
                    .putInt(localUDInformation.getQueuePairNumber())
                    .putInt(localUDInformation.getQueuePairKey())
                    .array();

            datagram = new DatagramPacket(buffer, buffer.length, InetAddress.getByName("255.255.255.255"), port);
        }

        @Override
        public void run() {
            while(isRunning) {
                try {
                    datagramSocket.send(datagram);
                } catch (Exception e) {
                    LOGGER.error("Could not broadcast UD information");
                }

                try {
                    Thread.sleep(sleepTime);
                } catch (InterruptedException e) {
                    LOGGER.info("Sleeping interrupted");
                }
            }
        }
    }
}
