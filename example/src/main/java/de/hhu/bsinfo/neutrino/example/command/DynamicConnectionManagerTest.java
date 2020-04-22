package de.hhu.bsinfo.neutrino.example.command;

import de.hhu.bsinfo.neutrino.buffer.RegisteredBuffer;
import de.hhu.bsinfo.neutrino.connection.dynamic.DynamicConnectionManager;
import de.hhu.bsinfo.neutrino.connection.statistic.Statistic;
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
    private static final int DEFAULT_BUFFER_SIZE = 32*1024*1024;
    private static final int DEFAULT_DEVICE_ID = 0;
    private static final int DEFAULT_ITERATIONS = 40;
    private static final int DEFAULT_THREAD_COUNT = 2;

    private DynamicConnectionManager dcm;
    private CyclicBarrier barrier;

    private AtomicLong startTime;
    private AtomicLong endTime;

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

    @Override
    public Void call() throws Exception {

        var statistics = new StatisticManager();

        dcm = new DynamicConnectionManager(port, bufferSize, statistics);
        dcm.init();

        startTime = new AtomicLong(0);
        endTime = new AtomicLong(0);

        var data = dcm.allocRegisteredBuffer(DEFAULT_DEVICE_ID, bufferSize);
        data.clear();

        var string = new NativeString(data, 0, bufferSize);
        string.set("Node " + dcm.getLocalId());

        TimeUnit.SECONDS.sleep(2);

        var remoteLids = dcm.getRemoteLocalIds();

        var workloads = new WorkloadExecutor[remoteLids.length];
        var executor = new PriorityScheduler(remoteLids.length * threadCount);


        barrier = new CyclicBarrier(remoteLids.length * threadCount);

        for (short remoteLid : remoteLids) {
            for(int i = 0; i < threadCount; i++) {
                executor.submit(new WorkloadExecutor(data, 0, remoteLid));
            }
        }

        long expectedOperationCount = (long) threadCount * iterations * remoteLids.length;

        while (expectedOperationCount > statistics.getTotalRDMAWriteCount()) {
        }

        long bytes = statistics.getTotalRDMABytesWritten();
        long count = statistics.getTotalRDMAWriteCount();

        endTime.set(System.nanoTime());

        var time = endTime.get() - startTime.get();

        var result = new Result(count, bytes, time, 0);


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

    private class WorkloadExecutor implements Runnable {
        private RegisteredBuffer data;
        private long offset;
        private short remoteLocalId;

        public WorkloadExecutor(RegisteredBuffer data, long offset, short remoteLocalId) {
            this.data = data;
            this.offset = offset;
            this.remoteLocalId = remoteLocalId;
        }

        @Override
        public void run() {
            var remoteBuffer = dcm.getRemoteBuffer(remoteLocalId);

            try {
                barrier.await();

                LOGGER.info("START REMOTE WRITE ON {}", remoteLocalId);

                startTime.compareAndSet(0, System.nanoTime());

                for(int i = 0; i < iterations; i++) {
                    dcm.remoteWrite(data, offset, bufferSize, remoteBuffer, remoteLocalId);
                }

                LOGGER.info("FINISHED REMOTE WRITE ON {}", remoteLocalId);

            } catch (Exception e) {
                LOGGER.error("Could not complete workload on {}", remoteLocalId);
            }
        }
    }
}
