package de.hhu.bsinfo.neutrino.connection.util;

import de.hhu.bsinfo.neutrino.connection.UnreliableDatagram;
import de.hhu.bsinfo.neutrino.connection.interfaces.InformationExchanger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;

/**
 * Exchanges information about UD queue pairs via TCP socket.
 * Supports no connect operation since UD queue pairs cannot be connected.
 *
 * @author Christian Gesse
 */
public class SocketUDInformationExchanger implements InformationExchanger<UDInformation> {
    private static final Logger LOGGER = LoggerFactory.getLogger(SocketUDInformationExchanger.class);

    /**
     * Socket used for information exchange
     */
    private final Socket socket;
    /**
     * Local UD queue pair
     */
    private final UnreliableDatagram unreliableDatagram;

    /**
     * Instantiates a new Socket UD information exchanger.
     *
     * @param socket             the socket
     * @param unreliableDatagram the unreliable datagram
     */
    public SocketUDInformationExchanger(Socket socket, UnreliableDatagram unreliableDatagram) {
        this.socket = socket;
        this.unreliableDatagram = unreliableDatagram;
    }

    /**
     * Instantiates a new Socket ud information exchanger.
     *
     * @param address            the ip address of the remote
     * @param unreliableDatagram the unreliable datagram
     * @throws IOException thrown if exception occurs creating socket
     */
    public SocketUDInformationExchanger(InetSocketAddress address, UnreliableDatagram unreliableDatagram) throws IOException {
        this.socket = new Socket(address.getAddress(), address.getPort());
        this.unreliableDatagram = unreliableDatagram;
    }

    /**
     * Exchange information about queue pairs with remote
     *
     * @return the information about the remote UD queue pair
     * @throws IOException thrown if exception occours using the socket
     */
    @Override
    public UDInformation exchangeInformation() throws IOException {
        sendLocalInformation();

        return getRemoteInformation();
    }

    /**
     * Gets information about remote UD queue pair from remote node
     *
     * @return the information about the remote UD queue pair
     * @throws IOException thrown if exception accours using the socket
     */
    @Override
    public UDInformation getRemoteInformation() throws IOException {

        LOGGER.info("Waiting for remote connection information");

        var byteBuffer = ByteBuffer.wrap(socket.getInputStream().readNBytes(UDInformation.SIZE));
        var remoteInfo = new UDInformation(byteBuffer);

        LOGGER.info("Received remote information: {}", remoteInfo);

        socket.close();

        return remoteInfo;
    }

    /**
     * Send information about local UD queue pair to remote node
     *
     * @throws IOException thrown if error occurs using socket
     */
    @Override
    public void sendLocalInformation() throws IOException {
        var localInfo = new UDInformation((byte) 1, unreliableDatagram.getPortAttributes().getLocalId(), unreliableDatagram.getQueuePair().getQueuePairNumber(), unreliableDatagram.getQueuePair().queryAttributes().getQkey());

        LOGGER.info("Local connection information: {}", localInfo);

        socket.getOutputStream().write(ByteBuffer.allocate(UDInformation.SIZE)
                .put(localInfo.getPortNumber())
                .putShort(localInfo.getLocalId())
                .putInt(localInfo.getQueuePairNumber())
                .putInt(localInfo.getQueuePairKey())
                .array());

    }
}
