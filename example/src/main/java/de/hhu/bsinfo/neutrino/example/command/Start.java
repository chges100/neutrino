package de.hhu.bsinfo.neutrino.example.command;

import de.hhu.bsinfo.neutrino.data.NativeArray;
import de.hhu.bsinfo.neutrino.data.NativeLinkedList;
import de.hhu.bsinfo.neutrino.verbs.AccessFlag;
import de.hhu.bsinfo.neutrino.verbs.CompletionQueue;
import de.hhu.bsinfo.neutrino.verbs.CompletionQueue.WorkCompletionArray;
import de.hhu.bsinfo.neutrino.verbs.Context;
import de.hhu.bsinfo.neutrino.verbs.Device;
import de.hhu.bsinfo.neutrino.verbs.MemoryRegion;
import de.hhu.bsinfo.neutrino.verbs.Mtu;
import de.hhu.bsinfo.neutrino.verbs.Port;
import de.hhu.bsinfo.neutrino.verbs.ProtectionDomain;
import de.hhu.bsinfo.neutrino.verbs.QueuePair;
import de.hhu.bsinfo.neutrino.verbs.QueuePair.AttributeMask;
import de.hhu.bsinfo.neutrino.verbs.QueuePair.Attributes;
import de.hhu.bsinfo.neutrino.verbs.QueuePair.State;
import de.hhu.bsinfo.neutrino.verbs.QueuePair.Type;
import de.hhu.bsinfo.neutrino.verbs.ReceiveWorkRequest;
import de.hhu.bsinfo.neutrino.verbs.ScatterGatherElement;
import de.hhu.bsinfo.neutrino.verbs.SendFlag;
import de.hhu.bsinfo.neutrino.verbs.SendWorkRequest;
import de.hhu.bsinfo.neutrino.verbs.SendWorkRequest.OpCode;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.StringJoiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

@CommandLine.Command(
    name = "start",
    description = "Starts a new neutrino instance.%n",
    showDefaultValues = true,
    separator = " ")
public class Start implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(Start.class);

    private static final int DEFAULT_QUEUE_SIZE = 100;
    private static final int DEFAULT_BUFFER_SIZE = 1024;
    private static final int DEFAULT_SERVER_PORT = 2998;

    private enum Transport {
        MESSAGE, RDMA
    }

    private Context context;
    private Device device;
    private Port port;
    private ProtectionDomain protectionDomain;
    private ByteBuffer buffer;
    private MemoryRegion memoryRegion;
    private CompletionQueue completionQueue;
    private QueuePair queuePair;

    private ConnectionInfo remoteInfo;

    @CommandLine.Option(
        names = "--server",
        description = "Runs this instance in server mode.")
    private boolean isServer;

    @CommandLine.Option(
        names = {"-p", "--port"},
        description = "The port the server will listen on.")
    private int portNumber = DEFAULT_SERVER_PORT;

    @CommandLine.Option(
        names = {"-b", "--buffer-size"},
        description = "Sets the memory regions buffer size.")
    private int bufferSize = DEFAULT_BUFFER_SIZE;

    @CommandLine.Option(
        names = {"-c", "--connect"},
        description = "The server to connect to.")
    private InetSocketAddress connection;

    @CommandLine.Option(
        names = {"-t", "--transport"},
        description = "The transport mode (MESSAGE, RDMA")
    private Transport transport = Transport.MESSAGE;

    @Override
    public void run() {
        if (!isServer && connection == null) {
            LOGGER.error("Please specify the server address");
            return;
        }

        int numDevices = Device.getDeviceCount();

        if(numDevices <= 0) {
            LOGGER.error("No RDMA devices were found in your system!");
            return;
        }

        context = Context.openDevice(0);
        LOGGER.info("Opened context for device {}!", context.getDeviceName());

        device = context.queryDevice();
        LOGGER.info(device.toString());

        port = context.queryPort(1);
        LOGGER.info(port.toString());

        protectionDomain = context.allocateProtectionDomain();
        LOGGER.info("Allocated protection domain!");

        buffer = ByteBuffer.allocateDirect(bufferSize);
        memoryRegion = protectionDomain.registerMemoryRegion(buffer, AccessFlag.LOCAL_WRITE, AccessFlag.REMOTE_READ, AccessFlag.REMOTE_WRITE);

        LOGGER.info("Registered memory region!");

        completionQueue = context.createCompletionQueue(DEFAULT_QUEUE_SIZE);
        LOGGER.info("Created completion queue!");

        try {
            if (isServer) {
                startServer();
                send();
                poll();
            } else {
                startClient();
                receive();
                poll();
                LOGGER.info("{}", buffer.getLong(0));
            }
        } catch (IOException e) {
            LOGGER.error(e.getMessage());
        }

        if (queuePair.destroy()) {
            LOGGER.info("Destroyed queue pair");
        }

        if (completionQueue.destroy()) {
            LOGGER.info("Destroyed completion queue");
        }

        if(memoryRegion.deregister()) {
            LOGGER.info("Deregistered memory region!");
        }

        if(protectionDomain.deallocate()) {
            LOGGER.info("Deallocated protection domain!");
        }

        if(context.close()) {
            LOGGER.info("Closed context!");
        }
    }

    private void startClient() throws IOException {
        var socket = new Socket(connection.getAddress(), connection.getPort());

        queuePair = createQueuePair(socket);

    }

    private void startServer() throws IOException {

        buffer.putLong(0, 0xC0FFEFEFEL);

        var serverSocket = new ServerSocket(portNumber);
        var socket = serverSocket.accept();

        queuePair = createQueuePair(socket);
    }

    private QueuePair createQueuePair(Socket socket) throws IOException {
        var initialAttributes = new QueuePair.InitialAttributes(config -> {
            config.setReceiveCompletionQueue(completionQueue);
            config.setSendCompletionQueue(completionQueue);
            config.setType(Type.RC);
            config.capabilities.setMaxSendWorkRequests(DEFAULT_QUEUE_SIZE);
            config.capabilities.setMaxReceiveWorkRequests(DEFAULT_QUEUE_SIZE);
            config.capabilities.setMaxReceiveScatterGatherElements(1);
            config.capabilities.setMaxSendScatterGatherElements(1);
        });

        queuePair = protectionDomain.createQueuePair(initialAttributes);

        LOGGER.info("Created queue pair!");

        var attributes = new QueuePair.Attributes(config -> {
            config.setState(State.INIT);
            config.setPartitionKeyIndex((short) 0);
            config.setPortNumber((byte) 1);
            config.setAccessFlags(AccessFlag.LOCAL_WRITE, AccessFlag.REMOTE_WRITE, AccessFlag.REMOTE_READ);
        });

        queuePair.modify(attributes, AttributeMask.STATE, AttributeMask.PKEY_INDEX, AttributeMask.PORT, AttributeMask.ACCESS_FLAGS);

        LOGGER.info("Queue pair transitioned to INIT state!");

        remoteInfo = exchangeInfo(socket, new ConnectionInfo(port.getLocalId(),
            queuePair.getQueuePairNumber(), memoryRegion.getRemoteKey(), memoryRegion.getAddress()));

        attributes = new Attributes(config -> {
            config.setState(State.RTR);
            config.setPathMtu(Mtu.IBV_MTU_4096);
            config.setDestination(remoteInfo.getQueuePairNumber());
            config.setReceivePacketNumber(0);
            config.setMaxDestinationAtomicReads((byte) 1);
            config.setMinRnrTimer((byte) 12);
            config.addressVector.setDestination(remoteInfo.getLocalId());
            config.addressVector.setServiceLevel((byte) 1);
            config.addressVector.setSourcePathBits((byte) 0);
            config.addressVector.setPortNumber((byte) 1);
            config.addressVector.setIsGlobal(false);
        });

        queuePair.modify(attributes, AttributeMask.STATE, AttributeMask.PATH_MTU, AttributeMask.DEST_QPN, AttributeMask.RQ_PSN, AttributeMask.AV, AttributeMask.MAX_DEST_RD_ATOMIC, AttributeMask.MIN_RNR_TIMER);

        LOGGER.info("Queue pair transitioned to RTR state");

        attributes = new Attributes(config -> {
            config.setState(State.RTS);
            config.setSendPacketNumber(0);
            config.setTimeout((byte) 14);
            config.setRetryCount((byte) 7);
            config.setRnrRetryCount((byte) 7);
            config.setMaxInitiatorAtomicReads((byte) 1);
        });

        queuePair.modify(attributes, AttributeMask.STATE, AttributeMask.SQ_PSN, AttributeMask.TIMEOUT, AttributeMask.RETRY_CNT, AttributeMask.RNR_RETRY, AttributeMask.MAX_QP_RD_ATOMIC);

        LOGGER.info("Queue pair transitioned to RTS state");

        return queuePair;
    }

    private void send() {

        var scatterGatherElements = new NativeArray<>(ScatterGatherElement::new, ScatterGatherElement.class, 1);
        scatterGatherElements.apply(0, scatterGatherElement -> {
            scatterGatherElement.setAddress(memoryRegion.getAddress());
            scatterGatherElement.setLength((int) memoryRegion.getLength());
            scatterGatherElement.setLocalKey(memoryRegion.getLocalKey());
        });

        var sendWorkRequest = new SendWorkRequest(request -> {
            if(transport == Transport.MESSAGE) {
                request.setOpCode(OpCode.SEND);
            } else if(transport == Transport.RDMA) {
                request.setOpCode(OpCode.RDMA_WRITE_WITH_IMM);
                request.rdma.setRemoteKey(remoteInfo.getRemoteKey());
                request.rdma.setRemoteAddress(remoteInfo.getRemoteAddress());
            }

            request.setFlags(SendFlag.SIGNALED);
            request.setListHandle(scatterGatherElements.getHandle());
            request.setListLength(scatterGatherElements.getCapacity());
        });

        var list = new NativeLinkedList<>(SendWorkRequest.LINKER);
        list.add(sendWorkRequest);

        queuePair.postSend(list);
    }

    private void receive() {

        var scatterGatherElements = new NativeArray<>(ScatterGatherElement::new, ScatterGatherElement.class, 1);
        scatterGatherElements.apply(0, scatterGatherElement -> {
            scatterGatherElement.setAddress(memoryRegion.getAddress());
            scatterGatherElement.setLength((int) memoryRegion.getLength());
            scatterGatherElement.setLocalKey(memoryRegion.getLocalKey());
        });

        var receiveWorkRequest = new ReceiveWorkRequest(request -> {
            request.setListHandle(scatterGatherElements.getHandle());
            request.setListLength(scatterGatherElements.getCapacity());
        });

        var list = new NativeLinkedList<>(ReceiveWorkRequest.LINKER);
        list.add(receiveWorkRequest);

        queuePair.postReceive(list);
    }

    private void poll() {

        var completionArray = new WorkCompletionArray(DEFAULT_QUEUE_SIZE);

        while (completionArray.getLength() == 0) {
            completionQueue.poll(completionArray);
        }
    }

    private static ConnectionInfo exchangeInfo(Socket socket, ConnectionInfo localInfo) throws IOException {

        LOGGER.info("Sending connection info {}", localInfo);
        socket.getOutputStream().write(ByteBuffer.allocate(Short.BYTES + 2 * Integer.BYTES + Long.BYTES)
            .putShort(localInfo.getLocalId())
            .putInt(localInfo.getQueuePairNumber())
            .putInt(localInfo.getRemoteKey())
            .putLong(localInfo.getRemoteAddress())
            .array());

        LOGGER.info("Waiting for remote connection info");
        var byteBuffer = ByteBuffer.wrap(socket.getInputStream().readNBytes(Short.BYTES + 2 * Integer.BYTES + Long.BYTES));

        var remoteInfo = new ConnectionInfo(byteBuffer.getShort(), byteBuffer.getInt(),
            byteBuffer.getInt(), byteBuffer.getLong());

        LOGGER.info("Received connection info {}", remoteInfo);

        return remoteInfo;
    }

    private static class ConnectionInfo {
        private final short localId;
        private final int queuePairNumber;
        private final int remoteKey;
        private final long remoteAddress;

        public ConnectionInfo(short localId, int queuePairNumber, int remoteKey, long remoteAddress) {
            this.localId = localId;
            this.queuePairNumber = queuePairNumber;
            this.remoteKey = remoteKey;
            this.remoteAddress = remoteAddress;
        }

        public short getLocalId() {
            return localId;
        }

        public int getQueuePairNumber() {
            return queuePairNumber;
        }

        public int getRemoteKey() {
            return remoteKey;
        }

        public long getRemoteAddress() {
            return remoteAddress;
        }

        @Override
        public String toString() {
            return new StringJoiner(", ", ConnectionInfo.class.getSimpleName() + "[", "]")
                .add("localId=" + localId)
                .add("queuePairNumber=" + queuePairNumber)
                .add("remoteKey=" + remoteKey)
                .add("remoteAddress=" + remoteAddress)
                .toString();
        }
    }
}
