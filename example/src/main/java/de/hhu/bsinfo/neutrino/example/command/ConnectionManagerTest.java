package de.hhu.bsinfo.neutrino.example.command;


import de.hhu.bsinfo.neutrino.buffer.BufferInformation;
import de.hhu.bsinfo.neutrino.connection.ConnectionManager;

import de.hhu.bsinfo.neutrino.connection.message.Message;
import de.hhu.bsinfo.neutrino.connection.message.MessageType;
import de.hhu.bsinfo.neutrino.connection.util.UDRemoteInformationExchanger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import sun.misc.Unsafe;

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

    @CommandLine.Option(
            names = {"-m", "--mode"},
            description = "Chosen test mode [0 -> ReliableConnection, 1 -> UnreliableDatagram.")
    private int mode;

    //TODO: Implement RDMA test

    @Override
    public Void call() throws Exception {
        if(!isServer && serverAddress == null) {
            LOGGER.error("Please specify the server address");
            return null;
        }

        if(isServer) {
            if(mode == 0) {
                startServerRC();
            } else if(mode == 1) {
                startServerUD();
            }
        } else {
            if(mode == 0) {
                startClientRC();
            } else if(mode == 1) {
                startClientUD();
            }
        }

        return null;
    }

    public void startServerRC() throws IOException {

        var serverSocket = new ServerSocket(port);
        var socket = serverSocket.accept();

        var connection = ConnectionManager.createReliableConnection(deviceId,  socket);

        LOGGER.info("Connection {} created", connection.getConnectionId());

        var message = new Message(connection.getDeviceContext(), MessageType.REMOTE_BUF_INFO, "2342535:322554:245");

        connection.send(message.getByteBuffer());

        int sent = 0;
        do{
            sent = connection.pollSend(1);
        } while(0 == sent);

        LOGGER.info("Send: {}", message);

        ConnectionManager.closeConnection(connection);

        LOGGER.info("Connection closed");
    }

    public void startClientRC() throws IOException {
        var connection = ConnectionManager.createReliableConnection(deviceId, serverAddress);

        LOGGER.info("Connection {} created", connection.getConnectionId());

        var buffer = ConnectionManager.allocLocalBuffer(connection.getDeviceContext(), Message.getSize());

        long wrId = connection.receive(buffer);

        int received = 0;
        do{
            received = connection.pollReceive(1);
        } while(0 == received);

        var message = new Message(buffer);

        var string = message.getPayload();
        String[] parts = string.split(":");

        var bufferInformation = new BufferInformation(Long.parseLong(parts[0]), Long.parseLong(parts[1]), Integer.parseInt(parts[2]));

        LOGGER.info("Received: {}", message);

        LOGGER.info("Received: {}", bufferInformation);

        ConnectionManager.closeConnection(connection);

        LOGGER.info("Connection closed");


    }

    public void startServerUD() throws IOException, Exception {

        var serverSocket = new ServerSocket(port);
        var socket = serverSocket.accept();

        var unreliableDatagram = ConnectionManager.createUnreliableDatagram(deviceId);
        var remoteInformation = new UDRemoteInformationExchanger(socket, unreliableDatagram).getRemoteInformation();

        LOGGER.info("Unreliable datagram created");

        var message = new Message(unreliableDatagram.getDeviceContext(), MessageType.REMOTE_BUF_INFO, "2342535:322554:245");

        // Only for test purpose
        TimeUnit.SECONDS.sleep(1);

        unreliableDatagram.send(message.getByteBuffer(), remoteInformation);

        int sent = 0;
        do{
            sent = unreliableDatagram.pollSend(1);
        } while(0 == sent);

        LOGGER.info("Send: {}", message);

        ConnectionManager.closeUnreliableDatagram(unreliableDatagram);

        LOGGER.info("Unreliable Datagram closed");
    }

    public void startClientUD() throws IOException {
        var unreliableDatagram = ConnectionManager.createUnreliableDatagram(deviceId);

        var remoteInfo = new UDRemoteInformationExchanger(serverAddress, unreliableDatagram).getRemoteInformation();

        LOGGER.info("Unreliable datagram created");

        var buffer = ConnectionManager.allocLocalBuffer(unreliableDatagram.getDeviceContext(), Message.getSize());

        long wrId = unreliableDatagram.receive(buffer);

        LOGGER.info("RWR id : {}", wrId);

        int received = 0;
        do{
            received = unreliableDatagram.pollReceive(1);
        } while(0 == received);

        var message = new Message(buffer);

        var string = message.getPayload();
        String[] parts = string.split(":");

        var bufferInformation = new BufferInformation(Long.parseLong(parts[0]), Long.parseLong(parts[1]), Integer.parseInt(parts[2]));

        LOGGER.info("Received: {}", message);

        LOGGER.info("Received: {}", bufferInformation);

        ConnectionManager.closeUnreliableDatagram(unreliableDatagram);

        LOGGER.info("Unreliable Datagram closed");


    }
}
