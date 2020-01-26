package de.hhu.bsinfo.neutrino.connection.util;

import de.hhu.bsinfo.neutrino.connection.UnreliableDatagram;
import de.hhu.bsinfo.neutrino.connection.interfaces.InformationExchanger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;

public class SocketUDInformationExchanger implements InformationExchanger<UDInformation> {
    private static final Logger LOGGER = LoggerFactory.getLogger(SocketUDInformationExchanger.class);

    private final Socket socket;
    private final UnreliableDatagram unreliableDatagram;

    public SocketUDInformationExchanger(Socket socket, UnreliableDatagram unreliableDatagram) {
        this.socket = socket;
        this.unreliableDatagram = unreliableDatagram;
    }

    public SocketUDInformationExchanger(InetSocketAddress address, UnreliableDatagram unreliableDatagram) throws IOException {
        this.socket = new Socket(address.getAddress(), address.getPort());
        this.unreliableDatagram = unreliableDatagram;
    }

    @Override
    public UDInformation exchangeInformation() throws IOException {
        sendLocalInformation();

        return getRemoteInformation();
    }

    @Override
    public UDInformation getRemoteInformation() throws IOException {

        LOGGER.info("Waiting for remote connection information");

        var byteBuffer = ByteBuffer.wrap(socket.getInputStream().readNBytes(UDInformation.SIZE));
        var remoteInfo = new UDInformation(byteBuffer);

        LOGGER.info("Received remote information: {}", remoteInfo);

        socket.close();

        return remoteInfo;
    }

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
