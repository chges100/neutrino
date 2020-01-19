package de.hhu.bsinfo.neutrino.example.command;


import de.hhu.bsinfo.neutrino.buffer.BufferInformation;
import de.hhu.bsinfo.neutrino.connection.ConnectionManager;

import de.hhu.bsinfo.neutrino.connection.message.Message;
import de.hhu.bsinfo.neutrino.connection.message.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.Callable;

@CommandLine.Command(
        name = "con-mgr-test",
        description = "Starts a simple test to see if the connection manager works properly.%n",
        showDefaultValues = true,
        separator = " ")
public class ConnectionManagerTest implements Callable<Void> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionManagerTest.class);

    private static final int DEFAULT_SERVER_PORT = 2998;
    private static final int DEFAULTBUFSIZE = 64;

    @CommandLine.Option(
            names = "--server",
            description = "Runs this instance in server mode.")
    private boolean isServer;

    @CommandLine.Option(
            names = {"-p", "--port"},
            description = "The port the server will listen on.")
    private int port = DEFAULT_SERVER_PORT;

    @CommandLine.Option(
            names = { "-d", "--device" },
            description = "Sets the InfiniBand device to be used.")
    private int deviceId = 0;

    @CommandLine.Option(
            names = {"-c", "--connect"},
            description = "The server to connect to.")
    private InetSocketAddress serverAddress;

    //TODO: Implement RDMA test

    @Override
    public Void call() throws Exception {
        if(!isServer && serverAddress == null) {
            LOGGER.error("Please specify the server address");
            return null;
        }

        if(isServer) {
            startServer();
        } else {
            startClient();
        }

        return null;
    }

    public void startServer() throws IOException {

        var serverSocket = new ServerSocket(port);
        var socket = serverSocket.accept();

        var connection = ConnectionManager.createReliableConnection(deviceId,  socket);

        socket.close();

        LOGGER.info("Connection {} created", connection.getConnectionId());

        var message = new Message(connection, MessageType.REMOTE_BUF_INFO, "2342535:322554:245");

        LOGGER.info("Send: {}", message);

        connection.send(message.getByteBuffer());
        connection.pollSend(1);

        ConnectionManager.closeConnection(connection);

        LOGGER.info("Connection closed");
    }

    public void startClient() throws IOException {
        var socket = new Socket(serverAddress.getAddress(), serverAddress.getPort());

        var connection = ConnectionManager.createReliableConnection(deviceId, socket);

        socket.close();

        LOGGER.info("Connection {} created", connection.getConnectionId());

        var buffer = ConnectionManager.allocLocalBuffer(connection.getDeviceContext(), Message.getSize());

        long wrId = connection.receive(buffer);

        int received = 0;
        do{
            received = connection.pollReceive(1);
        } while(0 == received);

        var message = new Message(connection, buffer);

        var string = message.getPayload();
        String[] parts = string.split(":");

        var bufferInformation = new BufferInformation(Long.parseLong(parts[0]), Long.parseLong(parts[1]), Integer.parseInt(parts[2]));

        LOGGER.info("Received: {}", message);

        LOGGER.info("Received: {}", bufferInformation);

        ConnectionManager.closeConnection(connection);

        LOGGER.info("Connection closed");


    }
}
