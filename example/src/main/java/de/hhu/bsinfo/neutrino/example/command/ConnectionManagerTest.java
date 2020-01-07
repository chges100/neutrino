package de.hhu.bsinfo.neutrino.example.command;


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
import java.util.concurrent.TimeUnit;

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

        var message = new Message(connection, MessageType.COMMON, "TEST1234");

        LOGGER.info(message.getPayload());

        LOGGER.info("Message: {}", message);

        connection.sendMessage(message);
        connection.pollSend(1);

        ConnectionManager.closeConnection(connection);

        LOGGER.info("Connection closed");
    }

    public void startClient() throws IOException {
        var socket = new Socket(serverAddress.getAddress(), serverAddress.getPort());

        var connection = ConnectionManager.createReliableConnection(deviceId, socket);

        socket.close();

        LOGGER.info("Connection {} created", connection.getConnectionId());

        var buffer = ConnectionManager.allocLocalBuffer(connection.getDeviceContext(), Message.getMessageSizeForPayload(8));

        long wrId = connection.receive(buffer);

        int received = 0;
        do{
            received = connection.pollReceive(1);
        } while(0 == received);

        var message = new Message(connection, buffer);

        LOGGER.info("Received Message: {}", message);

        ConnectionManager.closeConnection(connection);

        LOGGER.info("Connection closed");


    }
}
