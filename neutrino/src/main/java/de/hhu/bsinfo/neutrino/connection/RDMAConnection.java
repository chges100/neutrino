package de.hhu.bsinfo.neutrino.connection;

import de.hhu.bsinfo.neutrino.buffer.BufferInformation;
import de.hhu.bsinfo.neutrino.buffer.LocalBuffer;
import de.hhu.bsinfo.neutrino.buffer.RemoteBuffer;
import de.hhu.bsinfo.neutrino.connection.message.Message;
import de.hhu.bsinfo.neutrino.connection.message.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

public class RDMAConnection  extends ReliableConnection{
    private static final Logger LOGGER = LoggerFactory.getLogger(RDMAConnection.class);

    private static final int allocWaitTimeNanos = 500;
    private static final int initialReceiveWR = 5;

    private final ReliableConnection communicationChannel;
    private final HashMap<Long, RemoteBuffer> remoteBuffers;
    private final HashMap<Long, Message> receiveMessages;
    private final ArrayList<LocalBuffer> localBuffers;
    private final AtomicLong allocCounter;

    private static final ConnectionType connectionType = ConnectionType.RDMAConnection;

    public RDMAConnection(DeviceContext deviceContext) throws IOException {
        super(deviceContext);

        communicationChannel = ConnectionManager.createUnconnectedReliableConnection(getDeviceContext());
        remoteBuffers = new HashMap<>();
        receiveMessages = new HashMap<>();
        localBuffers = new ArrayList<>();
        allocCounter = new AtomicLong(0);
    }

    @Override
    public void connect(Socket socket) throws IOException {
        super.connect(socket);

        communicationChannel.connect(socket);

        LOGGER.info("Put {} initial Receive WorkRequests onto communication queue pair", initialReceiveWR);
        for(int i = 0; i < initialReceiveWR; i++) {
            communicationChannel.receiveMessage(new Message(communicationChannel, MessageType.REMOTE_BUF_ALLOC, ""));
        }
    }

    public RemoteBuffer allocRemoteBuffer(long capacity) {
        long id = allocCounter.getAndIncrement();
        LOGGER.info("Send alloc request with id {} and requested capacity {}", id, capacity);

        Message alloc_msg = new Message(this, MessageType.REMOTE_BUF_ALLOC, id + ":" + capacity);

        communicationChannel.sendMessage(alloc_msg);
        var receiveMessage = new Message(communicationChannel,MessageType.REMOTE_BUF_INFO,"");
        receiveMessages.put(communicationChannel.receiveMessage(receiveMessage), receiveMessage);

        LOGGER.info("Wait for answer to remote alloc request with id {}", id);

        while(!remoteBuffers.containsKey(id)) {
            LockSupport.parkNanos(allocWaitTimeNanos);
        }

        LOGGER.info("Remote buffer with id {} was allocated", id);

        return remoteBuffers.get(id);
    }

    private BufferInformation allocLocalBuffer(long capacity) {
        LOGGER.info("Allocate local buffer for remote reuqest with capacity {}", capacity);

        var buffer = ConnectionManager.allocLocalBuffer(getDeviceContext(), capacity);
        localBuffers.add(buffer);

        return new BufferInformation(buffer.getHandle(), buffer.capacity(), buffer.getRemoteKey());
    }

    public static ConnectionType getConnectionType() {
        return connectionType;
    }

    private class CommunicationReceiver extends Thread {
        private static final int batchSize = 10;
        @Override
        public void run() {

            while(true) {
                var receiveCompletions = communicationChannel.pollReceiveCompletions(batchSize);

                for(int i = 0; i < receiveCompletions.getLength(); i++) {
                    var wrId = receiveCompletions.get(i).getId();
                    var receiveMessage = receiveMessages.get(wrId);
                    receiveMessages.remove(wrId);

                    String[] payload = receiveMessage.getPayload().split(":");

                    if(receiveMessage.getMessageType() == MessageType.REMOTE_BUF_ALLOC) {
                        var bufInfo = allocLocalBuffer(Long.parseLong(payload[1]));

                        // put new receive wr onto queue
                        communicationChannel.receiveMessage(new Message(communicationChannel, MessageType.REMOTE_BUF_ALLOC, ""));

                        communicationChannel.sendMessage(new Message(communicationChannel, MessageType.REMOTE_BUF_INFO, payload[0] + ":" + bufInfo.getAddress() + ":" + bufInfo.getCapacity() + ":" + bufInfo.getRemoteKey()));

                        LOGGER.info("New buffer allocated answering alloc request with remote id {}", payload[0]);
                    } else if (receiveMessage.getMessageType() == MessageType.REMOTE_BUF_INFO) {
                        remoteBuffers.put(Long.parseLong(payload[0]), new RemoteBuffer(getQueuePair(), Long.parseLong(payload[1]), Long.parseLong(payload[2]), Integer.parseInt(payload[3])));
                        LOGGER.info("Received new remote buffer with id {}", payload[0]);
                    }

                }
            }

        }

    }
}

