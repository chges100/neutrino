package de.hhu.bsinfo.neutrino.connection.connector;

import de.hhu.bsinfo.neutrino.connection.util.ConnectionInformation;
import de.hhu.bsinfo.neutrino.connection.ReliableConnection;
import de.hhu.bsinfo.neutrino.connection.interfaces.Connector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;

public class SocketConnector implements Connector {

    private static final Logger LOGGER = LoggerFactory.getLogger(SocketConnector.class);


    private final Socket socket;
    private final ReliableConnection connection;

    public SocketConnector(Socket socket, ReliableConnection connection) {
        this.connection = connection;
        this.socket = socket;
    }

    public SocketConnector(InetSocketAddress address, ReliableConnection connection) throws IOException {
        this.connection = connection;
        this.socket = new Socket(address.getAddress(), address.getPort());
    }

    @Override
    public ConnectionInformation getConnectionInformation() throws IOException {
        var localInfo = new ConnectionInformation((byte) 1, connection.getPortAttributes().getLocalId(), connection.getQueuePair().getQueuePairNumber());

        LOGGER.info("Local connection information: {}", localInfo);

        socket.getOutputStream().write(ByteBuffer.allocate(Byte.BYTES + Short.BYTES + Integer.BYTES)
                .put(localInfo.getPortNumber())
                .putShort(localInfo.getLocalId())
                .putInt(localInfo.getQueuePairNumber())
                .array());

        LOGGER.info("Waiting for remote connection information");

        var byteBuffer = ByteBuffer.wrap(socket.getInputStream().readNBytes(Byte.BYTES + Short.BYTES + Integer.BYTES));
        var remoteInfo = new ConnectionInformation(byteBuffer);

        LOGGER.info("Received connection information: {}", remoteInfo);

        socket.close();

        return remoteInfo;
    }

    @Override
    public void connect() throws IOException {
        connection.connect(getConnectionInformation());
    }

}
