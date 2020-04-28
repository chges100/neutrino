package de.hhu.bsinfo.neutrino.example.command;

import de.hhu.bsinfo.neutrino.buffer.RegisteredBuffer;
import de.hhu.bsinfo.neutrino.connection.dynamic.DynamicConnectionManager;
import de.hhu.bsinfo.neutrino.connection.statistic.StatisticManager;
import de.hhu.bsinfo.neutrino.data.NativeString;
import de.hhu.bsinfo.neutrino.example.util.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import org.threadly.concurrent.*;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;


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
    private char mode = DEFAULT_THREAD_COUNT;

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

        var result = benchmarkThroughput(remoteLocalIds);

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

    private Result benchmarkThroughput(short ... remoteLocalIds) {

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

        long bytes = statistics.getTotalRDMABytesWritten();
        long count = statistics.getTotalRDMAWriteCount();

        var time = endTime.get() - startTime.get();

        return new Result(count, bytes, time, 0);
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
}
