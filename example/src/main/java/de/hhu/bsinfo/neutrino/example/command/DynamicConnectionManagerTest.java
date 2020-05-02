package de.hhu.bsinfo.neutrino.example.command;

import de.hhu.bsinfo.neutrino.buffer.RegisteredBuffer;
import de.hhu.bsinfo.neutrino.connection.dynamic.DynamicConnectionManager;
import de.hhu.bsinfo.neutrino.connection.statistic.StatisticManager;
import de.hhu.bsinfo.neutrino.data.NativeString;
import de.hhu.bsinfo.neutrino.example.measurement.LatencyMeasurement;
import de.hhu.bsinfo.neutrino.example.measurement.Measurement;
import de.hhu.bsinfo.neutrino.example.measurement.ThroughputMeasurement;
import org.agrona.collections.ArrayUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import org.threadly.concurrent.*;

import java.util.Arrays;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.LongStream;


@CommandLine.Command(
        name = "dyn-con-test",
        description = "Starts a simple test to see if the dynamic connection manager works properly.%n",
        showDefaultValues = true,
        separator = " ")
public class DynamicConnectionManagerTest implements Callable<Void> {

    private static final Logger LOGGER = LoggerFactory.getLogger(DynamicConnectionManagerTest.class);

    private static final int DEFAULT_SERVER_PORT = 2998;
    private static final int DEFAULT_BUFFER_SIZE = 2*1024*1024;
    private static final int DEFAULT_DEVICE_ID = 0;
    private static final int DEFAULT_ITERATIONS = 40;
    private static final int DEFAULT_THREAD_COUNT = 2;
    private static final int DEFAULT_MODE = 0;
    private static final int DEFAULT_SLEEP_INTERVAL = 6000;
    private static final long TIMEOUT = TimeUnit.NANOSECONDS.convert(10, TimeUnit.SECONDS);

    private DynamicConnectionManager dcm;
    private CyclicBarrier barrier;

    private PriorityScheduler executor = new PriorityScheduler(2);
    private StatisticManager statistics = new StatisticManager();
    private RegisteredBuffer data;

    @CommandLine.Option(
            names = {"-p", "--port"},
            description = "The port the server will listen on.")
    private int port = DEFAULT_SERVER_PORT;

    @CommandLine.Option(
            names = { "-d", "--device" },
            description = "Sets the InfiniBand device to be used.")
    private int deviceId = 0;

    @CommandLine.Option(
            names = { "-b", "--bufferSize" },
            description = "Sets the buffer size that should be used for RDMA.")
    private int bufferSize = DEFAULT_BUFFER_SIZE;

    @CommandLine.Option(
            names = { "-i", "--iterations" },
            description = "Sets the iteration count how often the buffer is written by every thread.")
    private int iterations = DEFAULT_ITERATIONS;

    @CommandLine.Option(
            names = { "-t", "--threadCount" },
            description = "Sets the amaount of threads for each remote.")
    private int threadCount = DEFAULT_THREAD_COUNT;

    @CommandLine.Option(
            names = { "-m", "--mode" },
            description = "Sets test mode:\n0 -> Throughput\n1 -> Latency")
    private int mode = DEFAULT_MODE;

    @CommandLine.Option(
            names = { "-s", "--sleepinterval" },
            description = "Sets default sleep interval for latency test")
    private int sleepInterval = DEFAULT_SLEEP_INTERVAL;

    @Override
    public Void call() throws Exception {

        dcm = new DynamicConnectionManager(port, bufferSize, statistics);
        dcm.init();

        data = dcm.allocRegisteredBuffer(DEFAULT_DEVICE_ID, bufferSize);
        data.clear();

        var string = new NativeString(data, 0, bufferSize);
        string.set("Node " + dcm.getLocalId());

        TimeUnit.SECONDS.sleep(2);

        var remoteLocalIds = dcm.getRemoteLocalIds();

        Measurement result = null;

        if(mode == 0) {
            result = benchmarkThroughput(remoteLocalIds);
        } else if(mode == 1) {
            result = benchmarkConnectLatency(remoteLocalIds);
        }


        try {
            executor.shutdown();
            executor.awaitTermination(500);
        } catch (Exception e) {
            LOGGER.info("Not all workload threads terminated");
        }

        dcm.shutdown();
        data.close();

        LOGGER.info(result.toString());

        return null;

    }

    private ThroughputMeasurement benchmarkThroughput(short ... remoteLocalIds) {

        var startTime = new AtomicLong(0);
        var endTime = new AtomicLong(0);

        executor.setPoolSize(remoteLocalIds.length * threadCount);

        barrier = new CyclicBarrier(remoteLocalIds.length * threadCount);

        var workloads = new WorkloadThroughput[remoteLocalIds.length];

        for (short remoteLid : remoteLocalIds) {
            statistics.registerRemote(remoteLid);
            for(int i = 0; i < threadCount; i++) {
                executor.submit(new WorkloadThroughput(data, 0, remoteLid, startTime));
            }
        }

        long expectedOperationCount = (long) threadCount * iterations * remoteLocalIds.length;

        while (expectedOperationCount > statistics.getTotalRDMAWriteCount()) {}

        endTime.set(System.nanoTime());

        long totalBytes = statistics.getTotalRDMABytesWritten();
        long operationCount = statistics.getTotalRDMAWriteCount();
        var operationSize = totalBytes / operationCount;

        var time = endTime.get() - startTime.get();

        var measurement = new ThroughputMeasurement(operationCount, operationSize);
        measurement.setMeasuredTime(time);

        return measurement;
    }

    private LatencyMeasurement benchmarkConnectLatency(short ... remoteLocalIds) {

        executor.setPoolSize(remoteLocalIds.length * threadCount);

        barrier = new CyclicBarrier(remoteLocalIds.length * threadCount);

        var workloads = new WorkloadLatency[remoteLocalIds.length];

        for (short remoteLid : remoteLocalIds) {
            statistics.registerRemote(remoteLid);
            for(int i = 0; i < threadCount; i++) {
                executor.submit(new WorkloadLatency(data, remoteLid, sleepInterval));
            }
        }

        long expectedOperationCount = (long) threadCount * iterations * remoteLocalIds.length;

        while (expectedOperationCount > statistics.getTotalRDMAWriteCount()) {}

        long totalBytes = statistics.getTotalRDMABytesWritten();
        long operationCount = statistics.getTotalRDMAWriteCount();
        var operationSize = totalBytes / operationCount;
        var connectLatencies = statistics.getConnectLatencies();

        var measurement = new LatencyMeasurement(operationCount, operationSize);

        if(connectLatencies.length > 0) {
            measurement.addLatencyMeasurement("Create Connection", connectLatencies);
        }

        return measurement;
    }


    private class WorkloadThroughput implements Runnable {
        private RegisteredBuffer data;
        private long offset;
        private short remoteLocalId;
        private AtomicLong timeStamp;

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

                LOGGER.info("START REMOTE WRITE ON {}", remoteLocalId);

                timeStamp.compareAndSet(0, System.nanoTime());

                for(int i = 0; i < iterations; i++) {
                    dcm.remoteWrite(data, offset, bufferSize, remoteBuffer, remoteLocalId);
                }

                LOGGER.info("FINISHED REMOTE WRITE ON {}", remoteLocalId);

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
