package de.hhu.bsinfo.neutrino.connection.util;

import de.hhu.bsinfo.neutrino.connection.ReliableConnection;
import de.hhu.bsinfo.neutrino.connection.UnreliableDatagram;
import de.hhu.bsinfo.neutrino.connection.connector.SocketConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;

public class UDRemoteInformationExchanger {
    private static final Logger LOGGER = LoggerFactory.getLogger(UDRemoteInformationExchanger.class);

    private final Socket socket;
    private final UnreliableDatagram unreliableDatagram;

    public UDRemoteInformationExchanger(Socket socket, UnreliableDatagram unreliableDatagram) {
        this.socket = socket;
        this.unreliableDatagram = unreliableDatagram;
    }

    public UDRemoteInformationExchanger(InetSocketAddress address, UnreliableDatagram unreliableDatagram) throws IOException {
        this.socket = new Socket(address.getAddress(), address.getPort());
        this.unreliableDatagram = unreliableDatagram;
    }

    public UDRemoteInformation getRemoteInformation() throws IOException {
        var localInfo = new UDRemoteInformation((byte) 1, unreliableDatagram.getPortAttributes().getLocalId(), unreliableDatagram.getQueuePair().getQueuePairNumber(), unreliableDatagram.getQueuePair().queryAttributes().getQkey());

        LOGGER.info("Local connection information: {}", localInfo);

        socket.getOutputStream().write(ByteBuffer.allocate(Byte.BYTES + Short.BYTES + Integer.BYTES + Integer.BYTES)
                .put(localInfo.getPortNumber())
                .putShort(localInfo.getLocalId())
                .putInt(localInfo.getQueuePairNumber())
                .putInt(localInfo.getQueuePairKey())
                .array());

        LOGGER.info("Waiting for remote connection information");

        var byteBuffer = ByteBuffer.wrap(socket.getInputStream().readNBytes(Byte.BYTES + Short.BYTES + Integer.BYTES + Integer.BYTES));
        var remoteInfo = new UDRemoteInformation(byteBuffer);

        LOGGER.info("Received connection information: {}", remoteInfo);

        socket.close();

        return remoteInfo;
    }
}
