package de.hhu.bsinfo.neutrino.example.command;

import de.hhu.bsinfo.neutrino.buffer.RegisteredBuffer;
import de.hhu.bsinfo.neutrino.connection.dynamic.DynamicConnectionManager;
import de.hhu.bsinfo.neutrino.connection.statistic.StatisticManager;
import de.hhu.bsinfo.neutrino.data.NativeString;
import de.hhu.bsinfo.neutrino.example.measurement.LatencyMeasurement;
import de.hhu.bsinfo.neutrino.example.measurement.Measurement;
import de.hhu.bsinfo.neutrino.example.measurement.ThroughputMeasurement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import org.threadly.concurrent.*;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;


/**
 * A simple test and benchmark command for the dynamic connection management.
 * Can also be regarded as an simple example how to use the dynamic connection management.
 *
 * @author Christian Gesse
 */
@CommandLine.Command(
        name = "dyn-con-test",
        description = "Starts a simple test to see if the dynamic connection manager works properly.%n",
        showDefaultValues = true,
        separator = " ")
public class DynamicConnectionManagerTest implements Callable<Void> {

    private static final Logger LOGGER = LoggerFactory.getLogger(DynamicConnectionManagerTest.class);

    private static final int DEFAULT_SERVER_PORT = 2998;
    private static final int DEFAULT_BUFFER_SIZE = 1024*1024;
    private static final int DEFAULT_DEVICE_ID = 0;
    private static final int DEFAULT_ITERATIONS = 40;
    private static final int DEFAULT_THREAD_COUNT = 2;
    private static final int DEFAULT_MODE = 0;
    private static final int DEFAULT_SLEEP_INTERVAL = 6000;
    private static final long TIMEOUT = TimeUnit.NANOSECONDS.convert(10, TimeUnit.SECONDS);

    /**
     * Instance of dynamic connection management
     */
    private DynamicConnectionManager dcm;
    /**
     * Barrier to start all workloads of worker threads simultaneously
     */
    private CyclicBarrier barrier;

    /**
     * Executor for worker threads
     */
    private PriorityScheduler executor = new PriorityScheduler(2);
    /**
     * Statistic manager to collect statistic data of dynamic connection management
     */
    private StatisticManager statistics = new StatisticManager();
    /**
     * Buffer used by all RDMA operations of this node
     */
    private RegisteredBuffer data;

    /**
     * The port the server will listen on
     */
    @CommandLine.Option(
            names = {"-p", "--port"},
            description = "The port the server will listen on.")
    private int port = DEFAULT_SERVER_PORT;

    /**
     * The InfiniBand device to be used
     */
    @CommandLine.Option(
            names = { "-d", "--device" },
            description = "Sets the InfiniBand device to be used.")
    private int deviceId = 0;

    /**
     * The buffer size that should be used for RDMA
     */
    @CommandLine.Option(
            names = { "-b", "--bufferSize" },
            description = "Sets the buffer size that should be used for RDMA.")
    private int bufferSize = DEFAULT_BUFFER_SIZE;

    /**
     * The iteration count how often the buffer is written by every thread
     */
    @CommandLine.Option(
            names = { "-i", "--iterations" },
            description = "Sets the iteration count how often the buffer is written by every thread.")
    private int iterations = DEFAULT_ITERATIONS;

    /**
     * The amaount of worker threads for each remote
     */
    @CommandLine.Option(
            names = { "-t", "--threadCount" },
            description = "Sets the amaount of worker threads for each remote.")
    private int threadCount = DEFAULT_THREAD_COUNT;
    /**
     * The benchmark mode
     */
    @CommandLine.Option(
            names = { "-m", "--mode" },
            description = "Sets test mode:\n0 -> Write Throughput\n1 -> Write Throughput\n2 -> Connect Latency\n3 -> Execute Latency")
    private int mode = DEFAULT_MODE;

    /**
     * Main function called by the command
     *
     * @return void
     * @throws Exception if exception occurs in connection management
     */
    @Override
    public Void call() throws Exception {

        // create new dynamic connection management and initialize it
        dcm = new DynamicConnectionManager(port, bufferSize, statistics);
        dcm.init();

        /* Now all instances of dynamic connection management in this network start to detect each other via UDP */

        // allocate registered buffer for RDMA operations and clear it
        data = dcm.allocRegisteredBuffer(DEFAULT_DEVICE_ID, bufferSize);
        data.clear();
        // add some content into the buffer (useful for debugging)
        var string = new NativeString(data, 0, bufferSize);
        string.set("Node " + dcm.getLocalId());

        // give all nodes some time to detect each other
        TimeUnit.SECONDS.sleep(4);

        // get a list of all detected remote nodes up to this point
        var remoteLocalIds = dcm.getRemoteLocalIds();

        // prepare measurement result
        Measurement result = null;

        // start selected benchmark and get result
        if(mode == 0) {
            result = benchmarkThroughput("Write", remoteLocalIds);
        } else if(mode == 1) {
            result = benchmarkThroughput("Read", remoteLocalIds);
        } else if(mode == 2) {
            result = benchmarkConnectLatency(remoteLocalIds);
        } else if(mode == 3) {
            result = benchmarkExecuteLatency(remoteLocalIds);
        }

        // shut down all worker threads
        try {
            executor.shutdown();
            executor.awaitTermination(500);
        } catch (Exception e) {
            LOGGER.info("Not all workload threads terminated");
        }

        // shut down dynamic connection management
        dcm.shutdown();
        data.close();

        // save measurement result to JSON file
        result.toJSON();

        LOGGER.info(result.toString());
        LOGGER.debug("FINISHED BENCHMARK with local id {}", dcm.getLocalId());

        return null;

    }

    private ThroughputMeasurement benchmarkThroughput(String throughputMode, short ... remoteLocalIds) {

        var startTime = new AtomicLong(0);
        var endTime = new AtomicLong(0);

        executor.setPoolSize(remoteLocalIds.length * threadCount);

        barrier = new CyclicBarrier(remoteLocalIds.length * threadCount);

        var workloads = new WorkloadThroughput[remoteLocalIds.length];

        for (short remoteLocalId : remoteLocalIds) {
            statistics.registerRemote(remoteLocalId);
            for(int i = 0; i < threadCount; i++) {
                executor.submit(new WorkloadThroughput(data, 0, remoteLocalId, startTime));
            }
        }

        long expectedOperationCount = (long) threadCount * iterations * remoteLocalIds.length;

        if(mode == 0) {
            while (expectedOperationCount > statistics.getTotalRDMAWriteCount()) {}
        } else if(mode == 1) {
            while (expectedOperationCount > statistics.getTotalRDMAReadCount()) {}
        }


        endTime.set(System.nanoTime());

        long totalBytes = 0;
        long operationCount = 0;

        if(mode == 0) {
            totalBytes = statistics.getTotalRDMABytesWritten();
            operationCount = statistics.getTotalRDMAWriteCount();
        } else if(mode == 1) {
            totalBytes = statistics.getTotalRDMABytesRead();
            operationCount = statistics.getTotalRDMAReadCount();
        }

        var operationSize = totalBytes / operationCount;

        var time = endTime.get() - startTime.get();

        var measurement = new ThroughputMeasurement(remoteLocalIds.length + 1, threadCount, dcm.getLocalId(), operationCount, operationSize, throughputMode);
        measurement.setMeasuredTime(time);

        return measurement;
    }

    private LatencyMeasurement benchmarkConnectLatency(short ... remoteLocalIds) {

        executor.setPoolSize(remoteLocalIds.length * threadCount);

        barrier = new CyclicBarrier(remoteLocalIds.length * threadCount);

        var workloads = new WorkloadLatency[remoteLocalIds.length];

        for (short remoteLocalId : remoteLocalIds) {
            statistics.registerRemote(remoteLocalId);
            for(int i = 0; i < threadCount; i++) {
                executor.submit(new WorkloadLatency(data, remoteLocalId, DEFAULT_SLEEP_INTERVAL));
            }
        }

        long expectedOperationCount = (long) threadCount * iterations * remoteLocalIds.length;

        while (expectedOperationCount > statistics.getTotalRDMAWriteCount()) {}

        long totalBytes = statistics.getTotalRDMABytesWritten();
        long operationCount = statistics.getTotalRDMAWriteCount();
        var operationSize = totalBytes / operationCount;
        var connectLatencies = statistics.getConnectLatencies();

        var measurement = new LatencyMeasurement(remoteLocalIds.length + 1, threadCount, dcm.getLocalId(), operationCount, operationSize);

        if(connectLatencies.length > 0) {
            measurement.addLatencyMeasurement("connect", connectLatencies);
        }

        return measurement;
    }

    private LatencyMeasurement benchmarkExecuteLatency(short ... remoteLocalIds) {

        var dummyLatency = new AtomicLong(0);

        dcm.activateExecuteLatencyMeasurement();

        executor.setPoolSize(remoteLocalIds.length * threadCount);

        barrier = new CyclicBarrier(remoteLocalIds.length * threadCount);

        var workloads = new WorkloadLatency[remoteLocalIds.length];

        for (short remoteLocalId : remoteLocalIds) {
            statistics.registerRemote(remoteLocalId);
            for(int i = 0; i < threadCount; i++) {
                executor.submit(new WorkloadThroughput(data, 0, remoteLocalId, dummyLatency));
            }
        }

        long expectedOperationCount = (long) threadCount * iterations * remoteLocalIds.length;

        while (expectedOperationCount > statistics.getTotalRDMAWriteCount()) {}

        long totalBytes = statistics.getTotalRDMABytesWritten();
        long operationCount = statistics.getTotalRDMAWriteCount();
        var operationSize = totalBytes / operationCount;
        var executeLatencies = statistics.getExecuteLatencies();

        var measurement = new LatencyMeasurement(remoteLocalIds.length + 1, threadCount, dcm.getLocalId(), operationCount, operationSize);

        if(executeLatencies.length > 0) {
            measurement.addLatencyMeasurement("execute", executeLatencies);
        }

        return measurement;
    }


    private class WorkloadThroughput implements Runnable {
        private RegisteredBuffer data;
        private long offset;
        private short remoteLocalId;
        private AtomicLong timeStamp;

        /**
         * Instantiates a new Workload throughput.
         *
         * @param data          the data
         * @param offset        the offset
         * @param remoteLocalId the remote local id
         * @param timeStamp     the time stamp
         */
        public WorkloadThroughput(RegisteredBuffer data, long offset, short remoteLocalId, AtomicLong timeStamp) {
            this.data = data;
            this.offset = offset;
            this.remoteLocalId = remoteLocalId;
            this.timeStamp = timeStamp;
        }

        @Override
        public void run() {

            try {
                var remoteBuffer = dcm.getRemoteBuffer(remoteLocalId);

                if(remoteBuffer == null) {
                    TimeUnit.MILLISECONDS.sleep(500);
                }

                barrier.await();

                LOGGER.info("START REMOTE OPERATION ON {}", remoteLocalId);

                timeStamp.compareAndSet(0, System.nanoTime());

                if(mode == 0 || mode == 3) {
                    for(int i = 0; i < iterations; i++) {
                        dcm.remoteWrite(data, offset, bufferSize, remoteBuffer, remoteLocalId);
                    }
                } else if(mode == 1) {
                    for(int i = 0; i < iterations; i++) {
                        dcm.remoteRead(data, offset, bufferSize, remoteBuffer, remoteLocalId);
                    }
                }



                LOGGER.info("FINISHED REMOTE OPERATION ON {}", remoteLocalId);

            } catch (Exception e) {
                LOGGER.error("Could not complete workload on {}", remoteLocalId);
                e.printStackTrace();
            }
        }
    }

    private class WorkloadLatency implements Runnable {
        private RegisteredBuffer data;
        private short remoteLocalId;
        private long sleepTimeMs;

        /**
         * Instantiates a new Workload latency.
         *
         * @param data          the data
         * @param remoteLocalId the remote local id
         * @param sleetTimeMs   the sleet time ms
         */
        public WorkloadLatency(RegisteredBuffer data, short remoteLocalId, long sleetTimeMs) {
            this.data = data;
            this.remoteLocalId = remoteLocalId;
            this.sleepTimeMs = sleetTimeMs;
        }

        @Override
        public void run() {

            try {
                var remoteBuffer = dcm.getRemoteBuffer(remoteLocalId);

                if(remoteBuffer == null) {
                    TimeUnit.MILLISECONDS.sleep(500);
                }

                LOGGER.info("START LATENCY TEST TO REMOTE {}", remoteLocalId);

                for(int i = 0; i < iterations; i++) {
                    barrier.await();
                    dcm.remoteWrite(data, 0, bufferSize, remoteBuffer, remoteLocalId);

                    TimeUnit.MILLISECONDS.sleep(sleepTimeMs);
                }

                LOGGER.info("FINISHED LATENCY TEST TO {}", remoteLocalId);

            } catch (Exception e) {
                LOGGER.error("Could not complete workload on {}", remoteLocalId);
                e.printStackTrace();
            }
        }
    }
}
