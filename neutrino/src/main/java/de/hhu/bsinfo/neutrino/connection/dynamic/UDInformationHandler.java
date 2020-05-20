package de.hhu.bsinfo.neutrino.connection.dynamic;

import de.hhu.bsinfo.neutrino.connection.util.UDInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.nio.ByteBuffer;

/**
 * Handler for information about dynamic connection handlers running on remote nodes
 *
 * @author Christian Gesse
 */
public class UDInformationHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(UDInformationHandler.class);

    /**
     * Instance of dynamic connection management
     */
    private final DynamicConnectionManager dcm;
    /**
     * UDP datagram socket used to propagate and recevie information about other nodes
     */
    private final DatagramSocket datagramSocket;

    /**
     * Instance of UD information receiver
     */
    private final UDInformationReceiver udReceiver;
    /**
     * Instance of UD information propagator
     */
    private final UDInformationPropagator udPropagator;

    /**
     * State of the handler
     */
    private boolean isRunning  = true;

    /**
     * Timeout for UDP datagram socket in ms
     */
    private final int timeoutMs = 500;
    /**
     * Port to use for UDP datagram socket
     */
    private final int port;

    /**
     * The (InfiniBand) local id of this node
     */
    private final short localId;

    /**
     * Instantiates a new UD information handler.
     *
     * @param dcm  instance of dynamic connection management
     * @param port port to use for UDP socket
     * @throws IOException thrown if exception occurs on UDP socket
     */
    protected UDInformationHandler(DynamicConnectionManager dcm, int port) throws IOException {
        this.dcm = dcm;
        this.port = port;
        this.localId = dcm.getLocalId();

        // create new datagram used for this UD information handler
        datagramSocket = new DatagramSocket(port);
        datagramSocket.setSoTimeout(timeoutMs);
        // enable broadcasting, since information about this node will be spread around in the network
        datagramSocket.setBroadcast(true);

        // create new information receiver and propagator
        udReceiver = new UDInformationReceiver();
        udPropagator = new UDInformationPropagator();
    }

    /**
     * Start background threads for UD information handler
     */
    protected void start() {
        udReceiver.start();
        udPropagator.start();
    }

    /**
     * Shutdown UD information handler
     */
    protected void shutdown() {
        isRunning = false;
    }

    /**
     * The UD information receiver polls the UDP datagram for incoming information about remote
     * nodes in the network and adds information about dynamic connection handlers running on them
     */
    private class UDInformationReceiver extends Thread {

        /**
         * Buffer for received data
         */
        private final DatagramPacket datagram;

        /**
         * Instantiates a new UD information receiver.
         *
         * @throws IOException the io exception
         */
        UDInformationReceiver() throws IOException {
            setName("UDInformationReceiver");

            // allocate new buffer for incoming data and create datagram
            var buffer = ByteBuffer.allocate(UDInformation.SIZE).array();
            datagram = new DatagramPacket(buffer, buffer.length);
        }

        /**
         * Run method of thread
         */
        @Override
        public void run() {
            while (isRunning) {
                // receive incoming information on UDP socket
                try {
                    datagramSocket.receive(datagram);
                } catch (Exception e) {
                    LOGGER.trace("Error receiving UDP datagram");
                }

                // wrap recevied data into buffer and create a UD information for the dectected remote dynamic connection handler
                var data = ByteBuffer.wrap(datagram.getData());
                var remoteInfo = new UDInformation(data);

                var remoteLocalId = remoteInfo.getLocalId();

                // register new remote dynamic connection handler
                if (!dcm.dch.hasRemoteHandlerInfo(remoteLocalId) && remoteLocalId != localId) {
                    // put remote handler info into map
                    dcm.dch.registerRemoteConnectionHandler(remoteLocalId, remoteInfo);
                    // create buffer for RDMA operations coming from this node
                    if (!dcm.localBufferHandler.hasBuffer(remoteLocalId)) {
                        dcm.localBufferHandler.createAndRegisterBuffer(remoteLocalId);
                    }

                    LOGGER.info("UDP: Add new remote connection handler {}", remoteInfo);
                }
            }
        }
    }

    /**
     * The UD information periodically propagator broadcasts information about the local dynamic connection handler
     * via an UDP socket
     */
    private class UDInformationPropagator extends Thread {
        /**
         * Datagram pakcet used for the broadcast
         */
        private final DatagramPacket datagram;

        /**
         * Standard period to sleep after broadcast
         */
        private final long sleepTimeMs = 1000;
        /**
         * Startup period to sleep after broadcast
         * (On initialization the information should be broadcast more frequently so that the node is detected earlier)
         */
        private final long startUpSleepTimeMs = 100;

        /**
         * Instantiates a new UD information propagator.
         *
         * @throws IOException the io exception
         */
        UDInformationPropagator() throws IOException {
            setName("UDInformationPropagator");

            // get information about local dynamic connection handler
            var localUDInformation = dcm.dch.getLocalUDInformation();

            // create new buffer and fill the data into it
            var buffer = ByteBuffer.allocate(UDInformation.SIZE)
                    .put(localUDInformation.getPortNumber())
                    .putShort(localUDInformation.getLocalId())
                    .putInt(localUDInformation.getQueuePairNumber())
                    .putInt(localUDInformation.getQueuePairKey())
                    .array();

            // create new datagram out of the buffer
            datagram = new DatagramPacket(buffer, buffer.length, InetAddress.getByName("255.255.255.255"), port);
        }

        /**
         * Run method of the thread
         */
        @Override
        public void run() {

            // on startup, use shorter period to broadcast information
            for(int i = 0; i < 20; i++) {
                try {
                    datagramSocket.send(datagram);
                } catch (Exception e) {
                    LOGGER.error("Could not broadcast UD information");
                }

                try {
                    Thread.sleep(startUpSleepTimeMs);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }

            // after startup, the standard period should be used
            while(isRunning) {
                try {
                    datagramSocket.send(datagram);
                } catch (Exception e) {
                    LOGGER.error("Could not broadcast UD information");
                }

                try {
                    Thread.sleep(sleepTimeMs);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }
}
