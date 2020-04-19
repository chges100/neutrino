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
    private static final int DEFAULT_BUFFER_SIZE = 1024;
    private static final int DEFAULT_DEVICE_ID = 0;
    private static final int ITERATIONS = 50;

    private DynamicConnectionManager dcm;

    @CommandLine.Option(
            names = {"-p", "--port"},
            description = "The port the server will listen on.")
    private int port = DEFAULT_SERVER_PORT;

    @CommandLine.Option(
            names = { "-d", "--device" },
            description = "Sets the InfiniBand device to be used.")
    private int deviceId = 0;

    @Override
    public Void call() throws Exception {

        dcm = new DynamicConnectionManager(port);
        dcm.init();

        var statistics = new StatisticManager();
        statistics.registerStatistic(Statistic.KeyType.REMOTE_LID, Statistic.Metric.RDMA_WRITE);
        statistics.registerStatistic(Statistic.KeyType.REMOTE_LID, Statistic.Metric.RDMA_BYTES_WRITTEN);
        statistics.registerStatistic(Statistic.KeyType.REMOTE_LID, Statistic.Metric.BYTES_SEND);

        dcm.registerStatisticManager(statistics);

        TimeUnit.SECONDS.sleep(2);

        var remoteLids = dcm.getRemoteLocalIds();
        var workloads = new WorkloadExecutor[remoteLids.length];
        var executor  = (ThreadPoolExecutor) Executors.newFixedThreadPool(remoteLids.length);

        for (short remoteLid : remoteLids) {
            executor.submit(new WorkloadExecutor(ITERATIONS, remoteLid));
        }

        TimeUnit.SECONDS.sleep(8);

        for (short remoteLid : remoteLids) {
            executor.submit(new WorkloadExecutor(ITERATIONS, remoteLid));
        }

        TimeUnit.SECONDS.sleep(2);

        statistics.shutDown();


        try {
            executor.shutdownNow();
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
        private int iterations = 1;

        public WorkloadExecutor(RegisteredBuffer data, long offset, short remoteLocalId) {
            this.data = data;
            this.offset = offset;
            this.remoteLocalId = remoteLocalId;
        }

        public WorkloadExecutor(int iterations, short remoteLocalId) {
            this.remoteLocalId = remoteLocalId;
            this.iterations = iterations;

            this.offset = 0;
            this.data = dcm.allocRegisteredBuffer(DEFAULT_DEVICE_ID, DEFAULT_BUFFER_SIZE);
            data.clear();
        }

        @Override
        public void run() {
            var string = new NativeString(data, 0, DEFAULT_BUFFER_SIZE);

            var remoteBuffer = dcm.getRemoteBuffer(remoteLocalId);

            LOGGER.debug("START REMOTE WRITE ON {}", remoteLocalId);
            try {
                for(int i = 0; i < iterations; i++) {
                    string.set("Node " + dcm.getLocalId() + " iter " + (i + 1));

                    dcm.remoteWrite(data, offset, DEFAULT_BUFFER_SIZE, remoteBuffer, remoteLocalId);
                }
            } catch (IOException e) {
                LOGGER.error("Could not complete workload on {}", remoteLocalId);
            }


            LOGGER.info("FINISHED REMOTE WRITE ON {}", remoteLocalId);

            data.close();
        }
    }
}
