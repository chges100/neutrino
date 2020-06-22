package de.hhu.bsinfo.neutrino.connection.dynamic;

import de.hhu.bsinfo.neutrino.buffer.BufferInformation;
import de.hhu.bsinfo.neutrino.buffer.RegisteredBuffer;
import de.hhu.bsinfo.neutrino.connection.DeviceContext;
import de.hhu.bsinfo.neutrino.connection.ReliableConnection;
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

/**
 * The dynamic connection manager. This is the core of the dynamic connection management.
 * It provides the api towards the applications that use the connection management.
 *
 * @author Christian Gesse
 */
public class DynamicConnectionManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(DynamicConnectionManager.class);

    /**
     * The batch size when polling the work completions
     */
    private static final int RC_COMPLETION_QUEUE_POLL_BATCH_SIZE = 200;
    /**
     * The timeout that is used for automatic disconnecting.
     * When a connection was not used for this period, ist is disconnected automatically.
     */
    private static final long RC_TIMEOUT_MS = 2000;
    /**
     * THis value is used as invalid local id for empty entries
     */
    private static final short INVALID_LID = Short.MAX_VALUE;
    /**
     * The timneout for obtaining the write lock during connection process
     */
    private static final long CREATE_CONNECTION_TIMEOUT_MS = 100;

    /**
     * The minimal size of the completion queue for all queue pairs
     */
    private static final int RC_MIN_COMPLETION_QUEUE_SIZE = 400;
    /**
     * The size of each queue pair created for a reliable connection
     */
    private static final int RC_QUEUE_PAIR_SIZE = 100;
    /**
     * The local id of this node
     */
    private final short localId;

    /**
     * A list of contexts for all devices
     */
    private final ArrayList<DeviceContext> deviceContexts = new ArrayList<>();

    /**
     * The instance of the dynamic conenction handler
     */
    protected DynamicConnectionHandler dch;


    /**
     * The completion queue for all reliable connection queue pairs
     */
    protected final CompletionQueue completionQueue;

    /**
     * The central connection table mapping local ids to reliable connections
     */
    protected final NonBlockingHashMapLong<ReliableConnection> connectionTable = new NonBlockingHashMapLong<>();;

    /**
     * The instance of the remote buffer handler
     */
    protected RemoteBufferHandler remoteBufferHandler;
    /**
     * The instance of the local buffer handler
     */
    protected LocalBufferHandler localBufferHandler;

    /**
     * The array with fast read write locks (one lock per possible local id)
     */
    protected final AtomicReadWriteLockArray rwLocks = new AtomicReadWriteLockArray(INVALID_LID);
    /**
     * Tbale indicating if the reliable connection to node with local id was used in the last timer period
     */
    protected final RCUsageTable rcUsageTable = new RCUsageTable(INVALID_LID);;

    /**
     * The central thread pool executor (scales its size dynamically)
     */
    protected final ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newCachedThreadPool();;

    /**
     * The size for all rdma buffers
     */
    protected final long rdmaBufferSize;

    /**
     * The instance of the information handler for remote connection handlers
     */
    private UDInformationHandler udInformationHandler;

    /**
     * Thread for polling the completion queue and processing the completions
     */
    private final RCCompletionQueuePollThread rcCompletionPoller;
    /**
     * Thread for automatic disconnecting of unused connections
     */
    private final RCDisconnectorThread rcDisconnector;

    /**
     * Indicating if latency before each rdma call shoul be measured
     * Might impact performance!
     */
    private boolean executeLatencyMeasurement = false;

    /**
     * The UDP port used for inital detection of remote nodes
     */
    private final int portUDP;

    /**
     * The instance of the statistic manager
     */
    protected final StatisticManager statisticManager;

    /**
     * Instantiates a new Dynamic connection manager.
     *
     * @param port             the UDP port used for initial detection
     * @param rdmaBufferSize   the rdma buffer size
     * @param statisticManager the instance of the statistic manager
     * @throws IOException thrown if exception occurs during context creation
     */
    public DynamicConnectionManager(int port, long rdmaBufferSize, StatisticManager statisticManager) throws IOException {

        this.rdmaBufferSize = rdmaBufferSize;
        this.statisticManager = statisticManager;
        portUDP = port;

        // get count of local InfiniBand devices
        var deviceCnt = Context.getDeviceCount();
        // create context for each device
        for (int i = 0; i < deviceCnt; i++) {
            var deviceContext = new DeviceContext(i);
            deviceContexts.add(deviceContext);
        }

        // there must exist at minimum one device context to work with
        if(deviceContexts.isEmpty()) {
            throw new IOException("Could not initialize any Infiniband device");
        }

        // get the local id of this node
        localId = deviceContexts.get(0).getContext().queryPort().getLocalId();
        LOGGER.info("Local Id is {}", localId);

        // create a completion queue used by all reliable connections
        completionQueue = deviceContexts.get(0).getContext().createCompletionQueue(RC_MIN_COMPLETION_QUEUE_SIZE);
        if(completionQueue == null) {
            throw new IOException("Cannot create completion queue");
        }

        // create the polling thread for the completion queue
        rcCompletionPoller = new RCCompletionQueuePollThread(RC_COMPLETION_QUEUE_POLL_BATCH_SIZE);
        rcCompletionPoller.start();

        // create the disconnector thread for unused connections
        rcDisconnector = new RCDisconnectorThread();
        rcDisconnector.start();
    }

    /**
     * Initialize dynamic connection management.
     *
     * @throws IOException the io exception
     */
    public void init() throws IOException{
        // create handler for buffers
        remoteBufferHandler = new RemoteBufferHandler(this);
        localBufferHandler= new LocalBufferHandler(this);

        // create the connection handler that handles meta communication with toher nodes
        LOGGER.trace("Create UD to handle connection requests");
        dch = new DynamicConnectionHandler(this, deviceContexts.get(0), INVALID_LID);
        LOGGER.info("Data of UD: {}", dch);

        // start up detection of remote nodes via UDP
        udInformationHandler = new UDInformationHandler(this, portUDP);
        udInformationHandler.start();
    }

    /**
     * Remote write operation via RDMA.
     *
     * @param data          the buffer containing the data
     * @param offset        the offset into the buffer
     * @param length        the length of the data to process
     * @param remoteBuffer  the information about the remote rdma buffer
     * @param remoteLocalId the local id of the remote node to operate on
     * @throws IOException the io exception
     */
    public void remoteWrite(RegisteredBuffer data, long offset, long length, BufferInformation remoteBuffer, short remoteLocalId) throws IOException {
        remoteExecute(SendWorkRequest.OpCode.RDMA_WRITE, data, offset, length, remoteBuffer, remoteLocalId);
    }

    /**
     * Remote read operation via RDMA.
     *
     * @param data          the buffer containing the data
     * @param offset        the offset into the buffer
     * @param length        the length of the data to process
     * @param remoteBuffer  the information about the remote rdma buffer
     * @param remoteLocalId the local id of the remote node to operate on
     * @throws IOException the io exception
     */
    public void remoteRead(RegisteredBuffer data, long offset, long length, BufferInformation remoteBuffer, short remoteLocalId) throws IOException {
        remoteExecute(SendWorkRequest.OpCode.RDMA_READ, data, offset, length, remoteBuffer, remoteLocalId);
    }

    /**
     * Execute RDMA operation on remote node.
     *
     * @param opCode        the opCode for the RDMA operatipn
     * @param data          the buffer containing the data
     * @param offset        the offset into the buffer
     * @param length        the length of the data to process
     * @param remoteBuffer  the information about the remote rdma buffer
     * @param remoteLocalId the local id of the remote node to operate on
     * @throws IOException the io exception
     */
    private void remoteExecute(SendWorkRequest.OpCode opCode, RegisteredBuffer data, long offset, long length, BufferInformation remoteBuffer, short remoteLocalId) throws IOException {
        // assume that no connection to remote exists
        boolean connected = false;
        ReliableConnection connection = null;

        // only used if latency measurement is activated
        long startTime = 0;
        long endTime = 0;

        // measure start time
        if(executeLatencyMeasurement) {
            startTime = System.nanoTime();
        }

        // main loop assuring that there exists a reliable connection to remote node
        while (!connected) {
            // create new connection if necessary
            if(!connectionTable.containsKey(remoteLocalId)) {
                createConnection(remoteLocalId);
            }

            // obtain read lock on this local id and get connection
            rwLocks.readLock(remoteLocalId);
            connection = connectionTable.get(remoteLocalId);

            // check if connection (still) exists and if it is connected
            if(connection != null && connection.isConnected()) {
                connected = true;

            } else {
                rwLocks.unlockRead(remoteLocalId);
            }
        }

        // mark this connection as used in this time period
        rcUsageTable.setUsed(remoteLocalId);

        // create latency measurement if necessary
        if(executeLatencyMeasurement) {
            endTime = System.nanoTime();

            var id = statisticManager.executeIdProvider.getAndIncrement();
            statisticManager.startExecuteLatencyStatistic(id, startTime);
            statisticManager.endExecuteLatencyStatistic(id, endTime);
        }

        // execute RDMA operation
        connection.execute(data, opCode, offset, length, remoteBuffer.getAddress(), remoteBuffer.getRemoteKey(), 0);

        // release one instance of the read lock
        rwLocks.unlockRead(remoteLocalId);

    }

    /**
     * Create a new reliable connection.
     *
     * @param remoteLocalId the local id of the remote node for the connection
     * @return boolean if connection was created successfully
     */
    protected boolean createConnection(short remoteLocalId) {
        // success indicator
        boolean createdConnection = false;

        // for latency measurement of connection creation
        long startConnect = System.nanoTime();

        // try to obtain exclusive write lock on local id - abort if not possible during until timeout
        var locked = rwLocks.writeLock(remoteLocalId, CREATE_CONNECTION_TIMEOUT_MS);

        // if lock is obtained and there (still) exists no connection, begin creation
        if(locked && !connectionTable.containsKey(remoteLocalId)) {
            try {
                // register remote into statistic manager
                statisticManager.registerRemote(remoteLocalId);

                // create new connection and put it into connection table
                var connection = new ReliableConnection(deviceContexts.get(0), RC_QUEUE_PAIR_SIZE, RC_QUEUE_PAIR_SIZE, completionQueue, completionQueue);
                connection.init();
                connectionTable.put(remoteLocalId, connection);

                // put start of latency measurement into statistics
                statisticManager.startConnectLatencyStatistic(connection.getId(), startConnect);

                // send new connection request to remote node - the further connection processed is handled asynchronously
                var localQP = new RCInformation(connection);
                dch.initConnectionRequest(localQP, remoteLocalId);

                // set success indicator
                createdConnection = true;

                // resize completion queue according to rc count
                var rcCount = connectionTable.keySet().size();
                if(completionQueue.getMaxElements() < RC_MIN_COMPLETION_QUEUE_SIZE + 2 * rcCount * RC_QUEUE_PAIR_SIZE) {
                    completionQueue.resize(RC_MIN_COMPLETION_QUEUE_SIZE + 2 * rcCount * RC_QUEUE_PAIR_SIZE);
                }

                // mark connection as used for the beginning
                rcUsageTable.setUsed(remoteLocalId);
            } catch (IOException e) {
                LOGGER.error("Could not create connection to {}", remoteLocalId);
            }
        }

        // release exclusive write lock
        if(locked) {
            rwLocks.unlockWrite(remoteLocalId);
        }

        return createdConnection;
    }

    /**
     * Allocate a registered buffer that can be used as buffer for RDMA operations.
     *
     * @param deviceId the device id for the context in which the buffer should be created
     * @param size     the size of the buffer
     * @return the registered buffer
     */
    public RegisteredBuffer allocRegisteredBuffer(int deviceId, long size) {
        return deviceContexts.get(deviceId).allocRegisteredBuffer(size);
    }

    /**
     * Gets the information about a remote buffer on a remote node
     *
     * @param remoteLocalId the local id of the remote node
     * @return the information about the remote buffer
     */
    public BufferInformation getRemoteBuffer(short remoteLocalId) {
        return remoteBufferHandler.getBufferInfo(remoteLocalId);
    }

    /**
     * Shutdown the dynamic connection manager.
     * It should not be used anymore after calling this method!
     *
     * @throws InterruptedException the interrupted exception
     */
    public void shutdown() throws InterruptedException {
        LOGGER.info("Shutdown dynamic connection manager");

        // first stop detection of new nodes
        udInformationHandler.shutdown();
        LOGGER.info("UDInformationHandler is shut down");

        LOGGER.info("Begin disconnecting all existing connections");

        // wait for all connections to be disconnected automatically
        while(!connectionTable.isEmpty()) {
            TimeUnit.SECONDS.sleep(2);
        }

        // shut down disconnector thread
        rcDisconnector.shutdown();
        LOGGER.info("RCDisconnector is shut down");

        // stop polling the completion queues since they are not needed any longer
        rcCompletionPoller.shutdown();
        LOGGER.info("RCCQPT is shut down");

        // shut down dynamic connection handler
        dch.shutdown();
        LOGGER.info("DCH is shut down");

        // shut down all threads
        executor.shutdown();

        try {
            executor.awaitTermination(500, TimeUnit.MILLISECONDS);
            LOGGER.info("Executor is shut down");
        } catch (InterruptedException e) {
            LOGGER.info("Thread Pool termination not yet finished - continue shutdown");
        }

        dch.close();
        LOGGER.info("DCH is closed");
    }

    /**
     * Gets local id of this node.
     *
     * @return the local id
     */
    public short getLocalId() {
        return localId;
    }

    /**
     * Gets all local ids of detected remote nodes.
     *
     * @return the array conatining all remote local ids
     */
    public short[] getRemoteLocalIds() {
        return dch.getRemoteLocalIds();
    }

    /**
     * Activate execute latency measurement.
     */
    public void activateExecuteLatencyMeasurement() {
        executeLatencyMeasurement = true;
    }

    /**
     * Disable execute latency measurement.
     */
    public void disableExecuteLatencyMeasurement() {
        executeLatencyMeasurement = false;
    }

    /**
     * Implementation of a thread that polls the completion queue in batches and processes the work completions
     *
     * @author Christian Gesse
     */
    private class RCCompletionQueuePollThread extends Thread {

        /**
         * Indicates state
         */
        private boolean isRunning = true;
        /**
         * Size of batches to poll
         */
        private final int batchSize;

        /**
         * Instantiates a new RC completion queue poll thread.
         *
         * @param batchSize the batch size to use
         */
        public RCCompletionQueuePollThread(int batchSize) {
            this.batchSize = batchSize;


        }

        /**
         * The run method of the thread.
         */
        @Override
        public void run() {
            // create a new array into which the batches of work coompletions are polled
            var completionArray = new CompletionQueue.WorkCompletionArray(RC_COMPLETION_QUEUE_POLL_BATCH_SIZE);
            // create a consumer for the work completions
            var completionConsumer = new WorkCompletionConsumer(completionArray);

            // poll
            while (isRunning) {
                // get a new batch of work completions
                completionQueue.poll(completionArray);

                // process work completions
                if(completionArray.getLength() > 0) {
                    completionConsumer.consume();
                }

            }
        }

        /**
         * Shutdown the poll thread.
         */
        public void shutdown() {
            isRunning = false;
        }
    }

    /**
     * Consumer for a batch of work completions.
     *
     * @author Christian Gesse
     */
    private final class WorkCompletionConsumer {
        private final CompletionQueue.WorkCompletionArray completionArray;

        /**
         * Instantiates a new Work completion consumer.
         *
         * @param completionArray the completion array containing the work completions
         */
        public WorkCompletionConsumer(CompletionQueue.WorkCompletionArray completionArray) {
            this.completionArray = completionArray;
        }

        /**
         * Main processing method.
         */
        public void consume() {
            // iterate through all completions
            for(int i = 0; i < completionArray.getLength(); i++) {

                // get completion and extract important information
                var completion = completionArray.get(i);

                var wrId = completion.getId();
                var opCode = completion.getOpCode();
                var qpNumber = completion.getQueuePairNumber();
                var status = completion.getStatus();

                // get missing information from the work request data
                var workRequestMapElement = ReliableConnection.fetchWorkRequestDataData(wrId);
                var connection = workRequestMapElement.connection;

                var remoteLocalId = workRequestMapElement.remoteLocalId;
                var bytes = workRequestMapElement.scatterGatherElement.getLength();
                var byteCount = completion.getByteCount();

                // acknowledge send completions since they are used to prevent overflow of queue pairs
                // and release work request information into buffer pool
                if(opCode.getValue() < 128) {
                    connection.acknowledgeSendCompletion();
                    workRequestMapElement.sendWorkRequest.releaseInstance();
                } else {
                    connection.acknowledgeReceiveCompletion();
                    workRequestMapElement.receiveWorkRequest.releaseInstance();
                }

                // release into buffer pool
                workRequestMapElement.scatterGatherElement.releaseInstance();
                workRequestMapElement.releaseInstance();

                // handle different opcodes and states of the work completion and put data into statistic manager
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
                    } else {

                        statisticManager.putOtherOpEvent(remoteLocalId);
                    }
                // special handling if work completion indicates that work request was not successful
                } else {
                    LOGGER.error("Work completiom failed: {}\n{} with OpCode {}", completion.getStatus(), completion.getStatusMessage(), completion.getOpCode());

                    // inform possible handshakes of connections about failing
                    if(opCode == WorkCompletion.OpCode.SEND) {
                        connection.getHandshakeQueue().pushSendError();
                    }

                    statisticManager.putErrorEvent(remoteLocalId);

                    // try to reconnect the queue pair - all aborted work reuqest must be handled by the application
                    try{
                        connection.resetFromError();

                        dch.initConnectionRequest(new RCInformation(connection), remoteLocalId);
                    } catch (Exception e) {
                        LOGGER.error("Something went wrong recovering RC after error: {}", e.toString());
                    }
                }
            }
        }
    }

    /**
     * Thread that activates itself in periods and disconnects unused connections
     *
     * @author Christian Gesse
     */
    private final class RCDisconnectorThread extends Thread {
        /**
         * State
         */
        private boolean isRunning = true;

        /**
         * Instantiates a new RC disconnector thread.
         */
        public RCDisconnectorThread() {

        }

        /**
         * Run method of thread
         */
        @Override
        public void run() {
            while (isRunning) {

                // iterate through all active connections
                for(var remoteLocalId : connectionTable.keySet()) {
                    // check if connection was used in the last timer period and reset state for the next period
                    var used = rcUsageTable.getStatusAndReset((int) (long) remoteLocalId);

                    // if connection was unused, initiate disconnect process
                    if(used == 0) {
                        dch.initDisconnectRequest(localId,  (short) (long) remoteLocalId);
                    }
                }

                LOGGER.debug("Active reliable connections: {}", connectionTable.entrySet().toArray().length);

                // Sleep over the next time period
                try {
                    Thread.sleep(RC_TIMEOUT_MS);
                } catch (InterruptedException e) {
                    interrupt();
                }
            }
        }

        /**
         * Shutdown.
         */
        public void shutdown() {
            isRunning = false;
        }
    }
}