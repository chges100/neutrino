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

public class SocketRCInformationExchanger implements Connector, InformationExchanger<RCInformation> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SocketRCInformationExchanger.class);


    private final Socket socket;
    private final ReliableConnection connection;

    public SocketRCInformationExchanger(Socket socket, ReliableConnection connection) {
        this.connection = connection;
        this.socket = socket;
    }

    public SocketRCInformationExchanger(InetSocketAddress address, ReliableConnection connection) throws IOException {
        this.connection = connection;
        this.socket = new Socket(address.getAddress(), address.getPort());
    }

    @Override
    public RCInformation exchangeInformation() throws IOException {
        sendLocalInformation();

        return getRemoteInformation();
    }

    @Override
    public RCInformation getRemoteInformation() throws IOException {
        LOGGER.info("Waiting for remote connection information");

        var byteBuffer = ByteBuffer.wrap(socket.getInputStream().readNBytes(RCInformation.SIZE));
        var remoteInfo = new RCInformation(byteBuffer);

        LOGGER.info("Received connection information: {}", remoteInfo);

        socket.close();

        return remoteInfo;
    }

    @Override
    public void sendLocalInformation() throws IOException {
        var localInfo = new RCInformation((byte) 1, connection.getPortAttributes().getLocalId(), connection.getQueuePair().getQueuePairNumber());

        LOGGER.info("Local connection information: {}", localInfo);

        socket.getOutputStream().write(ByteBuffer.allocate(RCInformation.SIZE)
                .put(localInfo.getPortNumber())
                .putShort(localInfo.getLocalId())
                .putInt(localInfo.getQueuePairNumber())
                .array());
    }

    @Override
    public void connect() throws IOException {
        sendLocalInformation();
        connection.connect(getRemoteInformation());
    }

}
