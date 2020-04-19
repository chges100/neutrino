package de.hhu.bsinfo.neutrino.example.command;

import de.hhu.bsinfo.neutrino.buffer.RegisteredBuffer;
import de.hhu.bsinfo.neutrino.connection.dynamic.DynamicConnectionManager;
import de.hhu.bsinfo.neutrino.connection.statistic.Statistic;
import de.hhu.bsinfo.neutrino.connection.statistic.StatisticManager;
import de.hhu.bsinfo.neutrino.data.NativeString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;


@CommandLine.Command(
        name = "dyn-con-test",
        description = "Starts a simple test to see if the dynamic connection manager works properly.%n",
        showDefaultValues = true,
        separator = " ")
public class DynamicConnectionManagerTest implements Callable<Void> {

    private static final Logger LOGGER = LoggerFactory.getLogger(DynamicConnectionManagerTest.class);

    private static final int DEFAULT_SERVER_PORT = 2998;
    private static final int DEFAULT_BUFFER_SIZE = 2048;
    private static final int DEFAULT_DEVICE_ID = 0;
    private static final int DEFAULT_ITERATIONS = 50;
    private static final int DEFAULT_THREAD_COUNT = 2;

    private DynamicConnectionManager dcm;

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

        dcm = new DynamicConnectionManager(port, bufferSize);
        dcm.init();

        var statistics = new StatisticManager();
        statistics.registerStatistic(Statistic.KeyType.REMOTE_LID, Statistic.Metric.RDMA_WRITE);
        statistics.registerStatistic(Statistic.KeyType.REMOTE_LID, Statistic.Metric.RDMA_BYTES_WRITTEN);
        statistics.registerStatistic(Statistic.KeyType.REMOTE_LID, Statistic.Metric.BYTES_SEND);

        dcm.registerStatisticManager(statistics);

        TimeUnit.SECONDS.sleep(2);

        var remoteLids = dcm.getRemoteLocalIds();
        var workloads = new WorkloadExecutor[remoteLids.length];
        var executor  = (ThreadPoolExecutor) Executors.newFixedThreadPool(remoteLids.length * threadCount);

        for (short remoteLid : remoteLids) {
            for(int i = 0; i < threadCount; i++) {
                executor.submit(new WorkloadExecutor(remoteLid));
            }
        }

        /*TimeUnit.SECONDS.sleep(8);

        for (short remoteLid : remoteLids) {
            executor.submit(new WorkloadExecutor(remoteLid));
        }*/

        TimeUnit.SECONDS.sleep(4);

        statistics.shutDown();


        try {
            executor.shutdown();
            executor.awaitTermination(500, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            LOGGER.info("Not all workload threads terminated");
        }

        dcm.shutdown();

        statistics.printResults();

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

        public WorkloadExecutor(short remoteLocalId) {
            this.remoteLocalId = remoteLocalId;

            this.offset = 0;
            this.data = dcm.allocRegisteredBuffer(DEFAULT_DEVICE_ID, bufferSize);
            data.clear();
        }

        @Override
        public void run() {
            var string = new NativeString(data, 0, bufferSize);

            var remoteBuffer = dcm.getRemoteBuffer(remoteLocalId);

            LOGGER.debug("START REMOTE WRITE ON {}", remoteLocalId);
            try {
                for(int i = 0; i < iterations; i++) {
                    string.set("Node " + dcm.getLocalId() + " iter " + (i + 1));

                    dcm.remoteWrite(data, offset, bufferSize, remoteBuffer, remoteLocalId);
                }
            } catch (IOException e) {
                LOGGER.error("Could not complete workload on {}", remoteLocalId);
            }


            LOGGER.info("FINISHED REMOTE WRITE ON {}", remoteLocalId);

            data.close();
        }
    }
}
