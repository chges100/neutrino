package de.hhu.bsinfo.neutrino.connection.util;

import de.hhu.bsinfo.neutrino.connection.ReliableConnection;
import de.hhu.bsinfo.neutrino.connection.interfaces.Connector;
import de.hhu.bsinfo.neutrino.connection.interfaces.InformationExchanger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;

/**
 * Exchanges information about RC queue pairs via TCP socket and connects a reliable connection
 *
 * @author Christian Gesse
 */
public class SocketRCInformationExchanger implements Connector, InformationExchanger<RCInformation> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SocketRCInformationExchanger.class);

    /**
     * Socket used for information exchange
     */
    private final Socket socket;
    /**
     * Reliable connection to connect to remote queue pair
     */
    private final ReliableConnection connection;

    /**
     * Instantiates a new socket RC information exchanger.
     *
     * @param socket     the socket
     * @param connection the connection
     */
    public SocketRCInformationExchanger(Socket socket, ReliableConnection connection) {
        this.connection = connection;
        this.socket = socket;
    }

    /**
     * Instantiates a new socket RC information exchanger.
     *
     * @param address    the ip address of the remote
     * @param connection the connection
     * @throws IOException thrown if exception occurs creating socket
     */
    public SocketRCInformationExchanger(InetSocketAddress address, ReliableConnection connection) throws IOException {
        this.connection = connection;
        this.socket = new Socket(address.getAddress(), address.getPort());
    }

    /**
     * Exchange information about queue pairs with remote
     *
     * @return the information about the remote RC queue pair
     * @throws IOException thrown if exception occurs using the socket
     */
    @Override
    public RCInformation exchangeInformation() throws IOException {
        sendLocalInformation();

        return getRemoteInformation();
    }

    /**
     * Gets information about remote RC queue pair from remote node
     *
     * @return the information about the remote RC queue pair
     * @throws IOException thrown if exception accours using the socket
     */
    @Override
    public RCInformation getRemoteInformation() throws IOException {
        LOGGER.info("Waiting for remote connection information");

        // wrap received data into buffer and create RC information from it
        var byteBuffer = ByteBuffer.wrap(socket.getInputStream().readNBytes(RCInformation.SIZE));
        var remoteInfo = new RCInformation(byteBuffer);

        LOGGER.info("Received connection information: {}", remoteInfo);

        socket.close();

        return remoteInfo;
    }

    /**
     * Send information about local RC queue pair to remote node
     *
     * @throws IOException thrown if error occurs using socket
     */
    @Override
    public void sendLocalInformation() throws IOException {
        // create information about local RC queue pair
        var localInfo = new RCInformation((byte) 1, connection.getPortAttributes().getLocalId(), connection.getQueuePair().getQueuePairNumber());

        LOGGER.info("Local connection information: {}", localInfo);

        // send information to remote node
        socket.getOutputStream().write(ByteBuffer.allocate(RCInformation.SIZE)
                .put(localInfo.getPortNumber())
                .putShort(localInfo.getLocalId())
                .putInt(localInfo.getQueuePairNumber())
                .array());
    }

    /**
     * Exchange information about queue pairs and connect them
     *
     * @throws IOException thrown if error occurs using socket
     */
    @Override
    public void connect() throws IOException {
        sendLocalInformation();
        connection.connect(getRemoteInformation());
    }

}
