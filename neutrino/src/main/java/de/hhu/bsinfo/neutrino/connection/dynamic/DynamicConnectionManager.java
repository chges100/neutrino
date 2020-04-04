package de.hhu.bsinfo.neutrino.connection.dynamic;

import de.hhu.bsinfo.neutrino.buffer.RegisteredBuffer;
import de.hhu.bsinfo.neutrino.connection.DeviceContext;
import de.hhu.bsinfo.neutrino.connection.ReliableConnection;
import de.hhu.bsinfo.neutrino.connection.util.AtomicReadWriteLockArray;
import de.hhu.bsinfo.neutrino.connection.util.RCInformation;
import de.hhu.bsinfo.neutrino.data.NativeString;
import de.hhu.bsinfo.neutrino.verbs.*;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.Long2ObjectHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.IOException;
import java.util.ArrayList;

public class DynamicConnectionManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(DynamicConnectionManager.class);

    protected static final long BUFFER_SIZE = 1024*1024;
    private static final int RC_COMPLETION_QUEUE_POLL_BATCH_SIZE = 200;

    private static final int LOCAL_BUFFER_READ = 16;
    private static final short MAX_LID = Short.MAX_VALUE;

    private static final long CREATE_CONNECTION_TIMEOUT = 100;
    private static final long REMOTE_EXEC_PARK_TIME = 1000;

    private static final int RC_COMPLETION_QUEUE_SIZE = 2000;
    private static final int RC_QUEUE_PAIR_SIZE = 200;

    private final short localId;

    private final ArrayList<DeviceContext> deviceContexts;

    protected DynamicConnectionHandler dch;


    protected final CompletionQueue sendCompletionQueue;
    protected final CompletionQueue receiveCompletionQueue;
    protected final Int2ObjectHashMap<ReliableConnection> connections;
    protected final Int2ObjectHashMap<ReliableConnection> qpToConnection;

    protected RemoteBufferHandler remoteBufferHandler;
    protected LocalBufferHandler localBufferHandler;

    protected final AtomicReadWriteLockArray rwLocks;

    private UDInformationHandler udInformationHandler;

    private final RCCompletionQueuePollThread rcCqpt;

    private final int UDPPort;



    public DynamicConnectionManager(int port) throws IOException {

        deviceContexts = new ArrayList<>();

        connections = new Int2ObjectHashMap<>();
        qpToConnection = new Int2ObjectHashMap<>();

        rwLocks = new AtomicReadWriteLockArray(MAX_LID);

        var deviceCnt = Context.getDeviceCount();

        for (int i = 0; i < deviceCnt; i++) {
            var deviceContext = new DeviceContext(i);
            deviceContexts.add(deviceContext);
        }

        localId = deviceContexts.get(0).getContext().queryPort().getLocalId();
        LOGGER.debug("Local Id is {}", localId);

        if(deviceContexts.isEmpty()) {
            throw new IOException("Could not initialize any Infiniband device");
        }

        sendCompletionQueue = deviceContexts.get(0).getContext().createCompletionQueue(RC_COMPLETION_QUEUE_SIZE);
        if(sendCompletionQueue == null) {
            throw new IOException("Cannot create completion queue");
        }

        receiveCompletionQueue = deviceContexts.get(0).getContext().createCompletionQueue(RC_COMPLETION_QUEUE_SIZE);
        if(sendCompletionQueue == null) {
            throw new IOException("Cannot create completion queue");
        }

        UDPPort = port;

        rcCqpt = new RCCompletionQueuePollThread(RC_COMPLETION_QUEUE_POLL_BATCH_SIZE);
        rcCqpt.start();
    }

    public void init() throws IOException{
        remoteBufferHandler = new RemoteBufferHandler(this);
        localBufferHandler= new LocalBufferHandler(this);

        LOGGER.trace("Create UD to handle connection requests");
        dch = new DynamicConnectionHandler(this, deviceContexts.get(0));
        LOGGER.info("Data of UD: {}", dch);

        udInformationHandler = new UDInformationHandler(this, UDPPort);
        udInformationHandler.start();
    }

    public void remoteWriteToAll(RegisteredBuffer data, long offset, long length) throws IOException {
        for(var remoteLocalId : dch.getRemoteLocalIds()){
            remoteWrite(data, offset, length, remoteLocalId);
        }
    }

    public void remoteReadFromAll(RegisteredBuffer data, long offset, long length) throws IOException {
        for(var remoteLocalId : dch.getRemoteLocalIds()){
            remoteRead(data, offset, length, remoteLocalId);
        }
    }

    public void remoteWrite(RegisteredBuffer data, long offset, long length, short remoteLocalId) throws IOException {
        remoteExecute(SendWorkRequest.OpCode.RDMA_WRITE, data, offset, length, remoteLocalId);
    }

    public void remoteRead(RegisteredBuffer data, long offset, long length, short remoteLocalId) throws IOException {
        remoteExecute(SendWorkRequest.OpCode.RDMA_READ, data, offset, length, remoteLocalId);
    }

    private void remoteExecute(SendWorkRequest.OpCode opCode, RegisteredBuffer data, long offset, long length, short remoteLocalId) throws IOException {
        boolean connected = false;
        ReliableConnection connection = null;

        while (!connected) {
            if(!connections.containsKey(remoteLocalId)) {
                createConnection(remoteLocalId);
            }

            rwLocks.readLock(remoteLocalId);
            connection = connections.get(remoteLocalId);

            if(connection != null && connection.isConnected()) {
                connected = true;
            } else {
                rwLocks.unlockRead(remoteLocalId);
                //LockSupport.parkNanos(REMOTE_EXEC_PARK_TIME);
            }
        }

        var remoteBufferInfo = remoteBufferHandler.getBufferInfo(remoteLocalId);

        LOGGER.trace("Start EXEC on remote {} on connection {}", remoteLocalId, connection.getId());
        connection.execute(data, opCode, offset, length, remoteBufferInfo.getAddress(), remoteBufferInfo.getRemoteKey(), 0);
        LOGGER.trace("Finished EXEC on remote {} on  connection {}", remoteLocalId, connection.getId());

        rwLocks.unlockRead(remoteLocalId);

    }

    protected void createConnection(short remoteLocalId) {

        var locked = rwLocks.writeLock(remoteLocalId, CREATE_CONNECTION_TIMEOUT);

        if(locked && !connections.containsKey(remoteLocalId)) {
            try {
                var connection = new ReliableConnection(deviceContexts.get(0), RC_QUEUE_PAIR_SIZE, RC_QUEUE_PAIR_SIZE, sendCompletionQueue, receiveCompletionQueue);
                connection.init();
                connections.put(remoteLocalId, connection);
                qpToConnection.put(connection.getQueuePair().getQueuePairNumber(), connection);

                var localQP = new RCInformation(connection);
                dch.sendConnectionRequest(localQP, remoteLocalId);
            } catch (IOException e) {
                LOGGER.error("Could not create connection to {}", remoteLocalId);
            }
        }

        if(locked) {
            rwLocks.unlockWrite(remoteLocalId);
        }
    }

    public RegisteredBuffer allocRegisteredBuffer(int deviceId, long size) {
        return deviceContexts.get(deviceId).allocRegisteredBuffer(size);
    }

    public void shutdown() {
        LOGGER.debug("Shutdown dynamic connection manager");

        udInformationHandler.shutdown();
        LOGGER.debug("UDInformationHandler is shut down");

        rcCqpt.shutdown();
        LOGGER.debug("RCCQPT is shut down");


        dch.shutdown();
        LOGGER.debug("DCH is shut down");

        LOGGER.debug("Begin disconnecting all existing connections");

        for(var kv : connections.entrySet()) {
            if(kv.getValue().isConnected()) {
                var connection = kv.getValue();

                var t = new Thread(() -> {
                    try {
                        connection.disconnect();
                        connection.close();
                        connections.remove(kv.getKey());
                    } catch (IOException e) {
                        LOGGER.debug("Error disconnecting connection {}: {}", connection.getId(), e);
                    }
                });
            }
        }

        printRemoteBufferInfos();
        printLocalBufferInfos();
    }

    public short getLocalId() {
        return localId;
    }

    public short[] getRemoteLocalIds() {
        return dch.getRemoteLocalIds();
    }

    public void printRCInfos() {
        String out = "Print out reliable connection information:\n";

        for(var connection : connections.values()) {
            out += connection;
            out += "\n";
            out += "connected: " + connection.isConnected();
            out += "\n";
        }

        LOGGER.info(out);

    }

    public void printRemoteBufferInfos() {
        var remoteLocalIds = getRemoteLocalIds();

        String out = "Print out remote buffer information:\n";

        for(var remoteLocalId : remoteLocalIds) {
            var remoteBufferInfo = remoteBufferHandler.getBufferInfo(remoteLocalId);

            if(remoteBufferInfo != null) {
                out += "Remote " + remoteLocalId + ": ";
                out += remoteBufferInfo;
                out += "\n";
            }

        }

        LOGGER.info(out);

    }

    public void printLocalBufferInfos() {
        var remoteLocalIds = getRemoteLocalIds();

        String out = "Content of local connection RDMA buffers:\n";

        for(var remoteLocalId : remoteLocalIds) {

            var buffer = localBufferHandler.getBuffer(remoteLocalId);

            if(buffer != null) {
                out += "Buffer for remote with address " + buffer.getHandle() + ": ";
                var tmp = new NativeString(buffer, 0, LOCAL_BUFFER_READ);
                out += tmp.get() + "\n";
            }
        }

        LOGGER.info(out);
    }

    private class RCCompletionQueuePollThread extends Thread {
        private boolean isRunning = true;
        private final int batchSize;
        private final CompletionQueue.WorkCompletionArray completionArray;

        public RCCompletionQueuePollThread(int batchSize) {
            this.batchSize = batchSize;

            completionArray = new CompletionQueue.WorkCompletionArray(RC_COMPLETION_QUEUE_POLL_BATCH_SIZE);
        }

        @Override
        public void run() {
            try {
                while(isRunning) {
                    sendCompletionQueue.poll(completionArray);

                    for(int i = 0; i < completionArray.getLength(); i++) {
                        var completion = completionArray.get(i);

                        if(completion.getOpCode() == WorkCompletion.OpCode.SEND) {
                            var qpNumber = completion.getQueuePairNumber();

                            if(completion.getStatus() == WorkCompletion.Status.SUCCESS) {
                                qpToConnection.get(qpNumber).getHandshakeQueue().pushSendComplete();
                            } else {
                                qpToConnection.get(qpNumber).getHandshakeQueue().pushSendError();
                            }
                        }

                        if(completion.getStatus() != WorkCompletion.Status.SUCCESS) {
                            LOGGER.error("Send Work completiom failed: {}\n{}", completion.getStatus(), completion.getStatusMessage());
                        }

                    }

                    receiveCompletionQueue.poll(completionArray);

                    for(int i = 0; i < completionArray.getLength(); i++) {
                        var completion = completionArray.get(i);

                        if(completion.getStatus() != WorkCompletion.Status.SUCCESS) {
                            LOGGER.error("Receive Work completiom failed: {}\n{}", completion.getStatus(), completion.getStatusMessage());
                        } else {
                            var qpNumber = completion.getQueuePairNumber();
                            qpToConnection.get(qpNumber).getHandshakeQueue().pushReceiveComplete();

                        }
                    }
                }
            } catch (IllegalStateException e) {
                LOGGER.error("Illegal state exception {}", e);
                e.printStackTrace();
            }
        }

        public void shutdown() {
            isRunning = false;
        }
    }
}