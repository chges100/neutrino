package de.hhu.bsinfo.neutrino.connection.dynamic;

import de.hhu.bsinfo.neutrino.buffer.BufferInformation;
import de.hhu.bsinfo.neutrino.buffer.RegisteredBuffer;
import de.hhu.bsinfo.neutrino.connection.DeviceContext;
import de.hhu.bsinfo.neutrino.connection.ReliableConnection;
import de.hhu.bsinfo.neutrino.connection.statistic.RAWData;
import de.hhu.bsinfo.neutrino.connection.statistic.Statistic;
import de.hhu.bsinfo.neutrino.connection.statistic.StatisticManager;
import de.hhu.bsinfo.neutrino.connection.util.AtomicReadWriteLockArray;
import de.hhu.bsinfo.neutrino.connection.util.RCInformation;
import de.hhu.bsinfo.neutrino.data.NativeString;
import de.hhu.bsinfo.neutrino.verbs.*;
import org.agrona.collections.Int2ObjectHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DynamicConnectionManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(DynamicConnectionManager.class);

    protected static final long BUFFER_SIZE = 1024*1024;
    private static final int RC_COMPLETION_QUEUE_POLL_BATCH_SIZE = 200;

    private static final long RC_TIMEOUT = 2000;

    private static final int LOCAL_BUFFER_READ = 16;
    private static final short INVALID_LID = Short.MAX_VALUE;

    private static final long CREATE_CONNECTION_TIMEOUT = 100;
    private static final long REMOTE_EXEC_PARK_TIME = 1000;

    private static final int RC_COMPLETION_QUEUE_SIZE = 2000;
    private static final int RC_QUEUE_PAIR_SIZE = 200;

    private final short localId;

    private final ArrayList<DeviceContext> deviceContexts;

    protected DynamicConnectionHandler dch;


    protected final CompletionQueue sendCompletionQueue;
    protected final CompletionQueue receiveCompletionQueue;
    protected final Int2ObjectHashMap<ReliableConnection> connectionTable;
    protected final Int2ObjectHashMap<ReliableConnection> qpToConnection;

    protected RemoteBufferHandler remoteBufferHandler;
    protected LocalBufferHandler localBufferHandler;

    protected final AtomicReadWriteLockArray rwLocks;
    protected final RCUsageTable rcUsageTable;

    private UDInformationHandler udInformationHandler;

    private final RCCompletionQueuePollThread rcCompletionPoller;
    private final RCDisconnectorThread rcDisconnector;

    private final int portUDP;

    private final List<StatisticManager> statisticManagers;



    public DynamicConnectionManager(int port) throws IOException {

        deviceContexts = new ArrayList<>();

        connectionTable = new Int2ObjectHashMap<>();
        qpToConnection = new Int2ObjectHashMap<>();

        statisticManagers = new ArrayList<>();

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

        sendCompletionQueue = deviceContexts.get(0).getContext().createCompletionQueue(RC_COMPLETION_QUEUE_SIZE);
        if(sendCompletionQueue == null) {
            throw new IOException("Cannot create completion queue");
        }

        receiveCompletionQueue = deviceContexts.get(0).getContext().createCompletionQueue(RC_COMPLETION_QUEUE_SIZE);
        if(sendCompletionQueue == null) {
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
                var connection = new ReliableConnection(deviceContexts.get(0), RC_QUEUE_PAIR_SIZE, RC_QUEUE_PAIR_SIZE, sendCompletionQueue, receiveCompletionQueue);
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

                if(remoteLid != INVALID_LID) {
                    dch.initDisconnectForce(localId, remoteLid);
                }
            }
        }

        dch.shutdown();
        LOGGER.debug("DCH is shut down");

        rcCompletionPoller.shutdown();
        LOGGER.debug("RCCQPT is shut down");


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

    public void registerStatisticManager(StatisticManager manager) {
        statisticManagers.add(manager);
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
            while(isRunning) {

                sendCompletionQueue.poll(completionArray);

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

                    workRequestMapElement.sendWorkRequest.releaseInstance();
                    workRequestMapElement.scatterGatherElement.releaseInstance();
                    workRequestMapElement.releaseInstance();

                    var statRAWData = new RAWData();

                    statRAWData.setKeyTypes(Statistic.KeyType.QP_NUM, Statistic.KeyType.CONNECTION_ID, Statistic.KeyType.REMOTE_LID);
                    statRAWData.setKeyData(completion.getQueuePairNumber(), wrId, remoteLocalId);

                    if(opCode == WorkCompletion.OpCode.SEND) {

                        if(status == WorkCompletion.Status.SUCCESS) {
                            connection.getHandshakeQueue().pushSendComplete();

                            statRAWData.setMetrics(Statistic.Metric.SEND, Statistic.Metric.BYTES_SEND, Statistic.Metric.SEND_QUEUE_SUCCESS);
                            statRAWData.setMetricsData(1, bytes, 1);
                        } else {
                            connection.getHandshakeQueue().pushSendError();

                            statRAWData.setMetrics(Statistic.Metric.SEND_QUEUE_ERRORS);
                            statRAWData.setMetricsData(1);
                        }
                    } else if(opCode == WorkCompletion.OpCode.RDMA_WRITE) {

                        if(status == WorkCompletion.Status.SUCCESS) {
                            statRAWData.setMetrics(Statistic.Metric.RDMA_WRITE, Statistic.Metric.RDMA_BYTES_WRITTEN, Statistic.Metric.SEND_QUEUE_SUCCESS);
                            statRAWData.setMetricsData(1, bytes, 1);

                        } else {

                            statRAWData.setMetrics(Statistic.Metric.SEND_QUEUE_ERRORS);
                            statRAWData.setMetricsData(1);
                        }
                    } else if(opCode == WorkCompletion.OpCode.RDMA_READ) {

                        if (status == WorkCompletion.Status.SUCCESS) {
                            statRAWData.setMetrics(Statistic.Metric.RDMA_READ, Statistic.Metric.RDMA_BYTES_READ, Statistic.Metric.SEND_QUEUE_SUCCESS);
                            statRAWData.setMetricsData(1, bytes, 1);

                        } else {

                            statRAWData.setMetrics(Statistic.Metric.SEND_QUEUE_ERRORS);
                            statRAWData.setMetricsData(1);
                        }
                    }

                    if(status != WorkCompletion.Status.SUCCESS) {
                        LOGGER.error("Send Work completiom failed: {}\n{}", completion.getStatus(), completion.getStatusMessage());

                        try{
                            connection.reset();
                            connection.init();

                            dch.initConnectionRequest(new RCInformation(connection), remoteLocalId);
                        } catch (Exception e) {
                            LOGGER.error("Something went wrong recovering RC after error: {}", e.toString());
                        }
                    }

                    for(var statisticManager : statisticManagers) {
                        statisticManager.pushRAWData(statRAWData);
                    }
                }

                receiveCompletionQueue.poll(completionArray);

                for(int i = 0; i < completionArray.getLength(); i++) {

                    var completion = completionArray.get(i);

                    var wrId = completion.getId();
                    var status = completion.getStatus();
                    var qpNumber = completion.getQueuePairNumber();

                    var workRequestMapElement = ReliableConnection.fetchWorkRequestDataData(wrId);
                    var connection = qpToConnection.get(qpNumber);


                    var remoteLocalId = workRequestMapElement.remoteLocalId;
                    var bytes = workRequestMapElement.scatterGatherElement.getLength();

                    workRequestMapElement.receiveWorkRequest.releaseInstance();
                    workRequestMapElement.scatterGatherElement.releaseInstance();
                    workRequestMapElement.releaseInstance();

                    var statRAWData = new RAWData();

                    statRAWData.setKeyTypes(Statistic.KeyType.QP_NUM, Statistic.KeyType.CONNECTION_ID, Statistic.KeyType.REMOTE_LID);
                    statRAWData.setKeyData(completion.getQueuePairNumber(), connection.getId(), connection.getRemoteLocalId());

                    if (status == WorkCompletion.Status.SUCCESS) {
                        connection.getHandshakeQueue().pushReceiveComplete();

                        statRAWData.setMetrics(Statistic.Metric.RECEIVE, Statistic.Metric.BYTES_RECEIVED, Statistic.Metric.RECEIVE_QUEUE_SUCCESS);
                        statRAWData.setMetricsData(1, bytes, 1);

                    } else {
                        LOGGER.error("Receive Work completiom failed: {}\n{}", completion.getStatus(), completion.getStatusMessage());

                        statRAWData.setMetrics(Statistic.Metric.RECEIVE_QUEUE_ERRORS);
                        statRAWData.setMetricsData(1);

                        try{
                            connection.reset();
                            connection.init();

                            dch.initConnectionRequest(new RCInformation(connection), remoteLocalId);
                        } catch (Exception e) {
                            LOGGER.error("Something went wrong recovering RC after error: {}", e.toString());
                        }
                    }

                    for(var statisticManager : statisticManagers) {
                        statisticManager.pushRAWData(statRAWData);
                    }
                }
            }

        }

        public void shutdown() {
            isRunning = false;
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
                    var used = rcUsageTable.getStatusAndReset(remoteLid);

                    if(used == 0) {
                        dch.initDisconnectRequest(localId,  (short) (int) remoteLid);
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