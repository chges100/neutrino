package de.hhu.bsinfo.neutrino.connection.dynamic;

import de.hhu.bsinfo.neutrino.buffer.BufferInformation;
import de.hhu.bsinfo.neutrino.buffer.RegisteredBuffer;
import de.hhu.bsinfo.neutrino.connection.DeviceContext;
import de.hhu.bsinfo.neutrino.connection.ReliableConnection;
import de.hhu.bsinfo.neutrino.connection.statistic.Statistic;
import de.hhu.bsinfo.neutrino.connection.statistic.StatisticManager;
import de.hhu.bsinfo.neutrino.connection.util.AtomicReadWriteLockArray;
import de.hhu.bsinfo.neutrino.connection.util.RCInformation;
import de.hhu.bsinfo.neutrino.data.NativeString;
import de.hhu.bsinfo.neutrino.verbs.*;
import org.jctools.maps.NonBlockingHashMapLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class DynamicConnectionManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(DynamicConnectionManager.class);

    private static final int RC_COMPLETION_QUEUE_POLL_BATCH_SIZE = 500;

    private static final long RC_TIMEOUT = 2000;

    private static final int LOCAL_BUFFER_READ = 16;
    private static final short INVALID_LID = Short.MAX_VALUE;

    private static final long CREATE_CONNECTION_TIMEOUT = 100;
    private static final long REMOTE_EXEC_PARK_TIME = 1000;

    private static final int RC_COMPLETION_QUEUE_SIZE = 600;
    private static final int RC_QUEUE_PAIR_SIZE = 100;

    private final short localId;

    private final ArrayList<DeviceContext> deviceContexts;

    protected DynamicConnectionHandler dch;


    protected final CompletionQueue completionQueue;

    protected final NonBlockingHashMapLong<ReliableConnection> connectionTable;
    protected final NonBlockingHashMapLong<ReliableConnection> qpToConnection;

    protected RemoteBufferHandler remoteBufferHandler;
    protected LocalBufferHandler localBufferHandler;

    protected final AtomicReadWriteLockArray rwLocks;
    protected final RCUsageTable rcUsageTable;

    protected final ThreadPoolExecutor executor;

    protected final long rdmaBufferSize;

    private UDInformationHandler udInformationHandler;

    private final RCCompletionQueuePollThread rcCompletionPoller;
    private final RCDisconnectorThread rcDisconnector;

    private final int portUDP;

    private final StatisticManager statisticManager;

    public DynamicConnectionManager(int port, long rdmaBufferSize, StatisticManager statisticManager) throws IOException {

        deviceContexts = new ArrayList<>();

        this.rdmaBufferSize = rdmaBufferSize;

        connectionTable = new NonBlockingHashMapLong<>();
        qpToConnection = new NonBlockingHashMapLong<>();

        this.statisticManager = statisticManager;
        executor = (ThreadPoolExecutor) Executors.newCachedThreadPool();


        rwLocks = new AtomicReadWriteLockArray(INVALID_LID);
        rcUsageTable = new RCUsageTable(INVALID_LID);

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

        completionQueue = deviceContexts.get(0).getContext().createCompletionQueue(RC_COMPLETION_QUEUE_SIZE);
        if(completionQueue == null) {
            throw new IOException("Cannot create completion queue");
        }

        portUDP = port;

        rcCompletionPoller = new RCCompletionQueuePollThread(RC_COMPLETION_QUEUE_POLL_BATCH_SIZE);
        rcCompletionPoller.start();

        rcDisconnector = new RCDisconnectorThread();
        rcDisconnector.start();
    }

    public void init() throws IOException{
        remoteBufferHandler = new RemoteBufferHandler(this);
        localBufferHandler= new LocalBufferHandler(this);

        LOGGER.trace("Create UD to handle connection requests");
        dch = new DynamicConnectionHandler(this, deviceContexts.get(0), INVALID_LID);
        LOGGER.info("Data of UD: {}", dch);

        udInformationHandler = new UDInformationHandler(this, portUDP);
        udInformationHandler.start();
    }

    public void remoteWrite(RegisteredBuffer data, long offset, long length, BufferInformation remoteBuffer, short remoteLocalId) throws IOException {
        remoteExecute(SendWorkRequest.OpCode.RDMA_WRITE, data, offset, length, remoteBuffer, remoteLocalId);
    }

    public void remoteRead(RegisteredBuffer data, long offset, long length, BufferInformation remoteBuffer, short remoteLocalId) throws IOException {
        remoteExecute(SendWorkRequest.OpCode.RDMA_READ, data, offset, length, remoteBuffer, remoteLocalId);
    }

    private void remoteExecute(SendWorkRequest.OpCode opCode, RegisteredBuffer data, long offset, long length, BufferInformation remoteBuffer, short remoteLocalId) throws IOException {
        boolean connected = false;
        ReliableConnection connection = null;

        while (!connected) {
            if(!connectionTable.containsKey(remoteLocalId)) {
                createConnection(remoteLocalId);
            }

            rwLocks.readLock(remoteLocalId);
            connection = connectionTable.get(remoteLocalId);

            if(connection != null && connection.isConnected()) {
                connected = true;
            } else {
                rwLocks.unlockRead(remoteLocalId);
            }
        }

        rcUsageTable.setUsed(remoteLocalId);

        connection.execute(data, opCode, offset, length, remoteBuffer.getAddress(), remoteBuffer.getRemoteKey(), 0);

        rwLocks.unlockRead(remoteLocalId);

    }

    protected void createConnection(short remoteLocalId) {

        var locked = rwLocks.writeLock(remoteLocalId, CREATE_CONNECTION_TIMEOUT);

        if(locked && !connectionTable.containsKey(remoteLocalId)) {
            try {
                var connection = new ReliableConnection(deviceContexts.get(0), RC_QUEUE_PAIR_SIZE, RC_QUEUE_PAIR_SIZE, completionQueue, completionQueue);
                connection.init();
                connectionTable.put(remoteLocalId, connection);
                qpToConnection.put(connection.getQueuePair().getQueuePairNumber(), connection);

                var localQP = new RCInformation(connection);
                dch.initConnectionRequest(localQP, remoteLocalId);

                rcUsageTable.setUsed(remoteLocalId);
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

    public BufferInformation getRemoteBuffer(short remoteLocalId) {
        return remoteBufferHandler.getBufferInfo(remoteLocalId);
    }

    public void shutdown() {
        LOGGER.debug("Shutdown dynamic connection manager");

        rcDisconnector.shutdown();
        LOGGER.debug("RCDisconnector is shut down");

        udInformationHandler.shutdown();
        LOGGER.debug("UDInformationHandler is shut down");

        LOGGER.debug("Begin disconnecting all existing connections");

        for(var kv : connectionTable.entrySet()) {

            if(kv.getValue().isConnected()) {
                var remoteLid = kv.getValue().getRemoteLocalId();

                if(kv.getValue().isConnected()) {
                    dch.initDisconnectForce(localId, remoteLid);
                }
            }
        }

        executor.shutdownNow();

        try {
            executor.awaitTermination(500, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            LOGGER.info("Thread Pool termination not yet finished - continue shutdown");
        }

        rcCompletionPoller.shutdown();
        LOGGER.debug("RCCQPT is shut down");

        dch.shutdown();
        LOGGER.debug("DCH is shut down");




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
        StringBuilder out = new StringBuilder("Print out reliable connection information:\n");

        for(var connection : connectionTable.values()) {
            out.append(connection);
            out.append("\n");
            out.append("connected: ").append(connection.isConnected());
            out.append("\n");
        }

        LOGGER.info(out.toString());

    }

    public void printRemoteBufferInfos() {
        var remoteLocalIds = getRemoteLocalIds();

        StringBuilder out = new StringBuilder("Print out remote buffer information:\n");

        for(var remoteLocalId : remoteLocalIds) {
            var remoteBufferInfo = remoteBufferHandler.getBufferInfo(remoteLocalId);

            if(remoteBufferInfo != null) {
                out.append("Remote ").append(remoteLocalId).append(": ");
                out.append(remoteBufferInfo);
                out.append("\n");
            }

        }

        LOGGER.info(out.toString());

    }

    public void printLocalBufferInfos() {
        var remoteLocalIds = getRemoteLocalIds();

        StringBuilder out = new StringBuilder("Content of local connection RDMA buffers:\n");

        for(var remoteLocalId : remoteLocalIds) {

            var buffer = localBufferHandler.getBuffer(remoteLocalId);

            if(buffer != null) {
                out.append("Buffer for remote with address ").append(buffer.getHandle()).append(": ");
                var tmp = new NativeString(buffer, 0, LOCAL_BUFFER_READ);
                out.append(tmp.get()).append("\n");
            }
        }

        LOGGER.info(out.toString());
    }

    private class RCCompletionQueuePollThread extends Thread {

        private boolean isRunning = true;
        private final int batchSize;

        public RCCompletionQueuePollThread(int batchSize) {
            this.batchSize = batchSize;


        }

        @Override
        public void run() {
            while (isRunning) {
                var completionArray = new CompletionQueue.WorkCompletionArray(RC_COMPLETION_QUEUE_POLL_BATCH_SIZE);

                completionQueue.poll(completionArray);

                if(completionArray.getLength() > 0) {
                    executor.execute(new WorkCompletionConsumer(completionArray));
                }

            }
        }

        public void shutdown() {
            isRunning = false;
        }
    }

    private final class WorkCompletionConsumer implements Runnable {
        private final CompletionQueue.WorkCompletionArray completionArray;

        public WorkCompletionConsumer(CompletionQueue.WorkCompletionArray workCompletionArray) {
            this.completionArray = workCompletionArray;
        }

        @Override
        public void run() {
            for(int i = 0; i < completionArray.getLength(); i++) {

                var completion = completionArray.get(i);

                var wrId = completion.getId();
                var opCode = completion.getOpCode();
                var qpNumber = completion.getQueuePairNumber();
                var status = completion.getStatus();

                var workRequestMapElement = ReliableConnection.fetchWorkRequestDataData(wrId);
                var connection = qpToConnection.get(qpNumber);

                var remoteLocalId = workRequestMapElement.remoteLocalId;
                var bytes = workRequestMapElement.scatterGatherElement.getLength();
                var byteCount = completion.getByteCount();

                if(opCode.getValue() < 128) {
                    workRequestMapElement.sendWorkRequest.releaseInstance();
                } else {
                    workRequestMapElement.receiveWorkRequest.releaseInstance();
                }

                workRequestMapElement.scatterGatherElement.releaseInstance();
                workRequestMapElement.releaseInstance();

                if(status == WorkCompletion.Status.SUCCESS) {
                    if(opCode == WorkCompletion.OpCode.SEND) {
                        connection.getHandshakeQueue().pushSendComplete();

                        statisticManager.putSendEvent(remoteLocalId, bytes);
                    } else if(opCode == WorkCompletion.OpCode.RECV) {
                        connection.getHandshakeQueue().pushReceiveComplete();

                        statisticManager.putReceiveEvent(remoteLocalId, byteCount);

                    } else if(opCode == WorkCompletion.OpCode.RDMA_WRITE) {

                        statisticManager.putRDMAWriteEvent(remoteLocalId, bytes);
                    } else if (opCode == WorkCompletion.OpCode.RDMA_READ) {

                        statisticManager.putRDMAReadEvent(remoteLocalId, bytes);
                    }

                } else {
                    LOGGER.error("Send Work completiom failed: {}\n{}", completion.getStatus(), completion.getStatusMessage());

                    if(opCode == WorkCompletion.OpCode.SEND) {
                        connection.getHandshakeQueue().pushSendError();
                    }

                    statisticManager.putErrorEvent(remoteLocalId);

                    try{
                        connection.reset();
                        connection.init();

                        dch.initConnectionRequest(new RCInformation(connection), remoteLocalId);
                    } catch (Exception e) {
                        LOGGER.error("Something went wrong recovering RC after error: {}", e.toString());
                    }
                }
            }
        }
    }

    private final class RCDisconnectorThread extends Thread {
        private boolean isRunning = true;

        public RCDisconnectorThread() {

        }

        @Override
        public void run() {
            while (isRunning) {

                for(var remoteLid : connectionTable.keySet()) {
                    var used = rcUsageTable.getStatusAndReset((int) (long) remoteLid);

                    if(used == 0) {
                        dch.initDisconnectRequest(localId,  (short) (long) remoteLid);
                    }
                }

                try {
                    Thread.sleep(RC_TIMEOUT);
                } catch (InterruptedException e) {
                    interrupt();
                }
            }
        }

        public void shutdown() {
            isRunning = false;
        }
    }
}