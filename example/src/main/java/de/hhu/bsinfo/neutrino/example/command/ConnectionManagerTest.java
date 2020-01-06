package de.hhu.bsinfo.neutrino.example.command;


import de.hhu.bsinfo.neutrino.connection.ConnectionManager;

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

    private ConnectionManager connectionManager;

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

        connectionManager = new ConnectionManager();

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

        var connection = connectionManager.createReliableConnection(deviceId,  socket);

        socket.close();

        LOGGER.info("Connection {} created", connection.getConnectionId());

        var buffer = connectionManager.allocLocalBuffer(connection.getDeviceContext(), DEFAULTBUFSIZE);
        LOGGER.info("Send magic number");
        buffer.putInt(0, 102401024);

        int magic = buffer.getInt(0);
        LOGGER.info("Magic number is {}", magic);

        connection.send(buffer);
        connection.pollSend(1);

        connectionManager.closeConnection(connection);

        LOGGER.info("Connection closed");
    }

    public void startClient() throws IOException {
        var socket = new Socket(serverAddress.getAddress(), serverAddress.getPort());

        var connection = connectionManager.createReliableConnection(deviceId, socket);

        socket.close();

        LOGGER.info("Connection {} created", connection.getConnectionId());

        var buffer = connectionManager.allocLocalBuffer(connection.getDeviceContext(), DEFAULTBUFSIZE);
        // Clear buffer
        buffer.putInt(0, 0);

        long wrId = connection.receive(buffer);

        int received = 0;
        do{
            received = connection.pollReceive(1);
        } while(0 == received);

        int receive = buffer.getInt(0);

        LOGGER.info("Received magic number {}", receive);

        connectionManager.closeConnection(connection);

        LOGGER.info("Connection closed");


    }
}
