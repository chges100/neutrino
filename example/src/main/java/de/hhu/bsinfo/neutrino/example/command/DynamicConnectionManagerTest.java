package de.hhu.bsinfo.neutrino.example.command;

import de.hhu.bsinfo.neutrino.buffer.RegisteredBuffer;
import de.hhu.bsinfo.neutrino.connection.dynamic.DynamicConnectionManager;
import de.hhu.bsinfo.neutrino.connection.dynamic.DynamicConnectionManagerOld;
import de.hhu.bsinfo.neutrino.data.NativeString;
import de.hhu.bsinfo.neutrino.verbs.SendWorkRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.StampedLock;

@CommandLine.Command(
        name = "dyn-con-test",
        description = "Starts a simple test to see if the dynamic connection manager works properly.%n",
        showDefaultValues = true,
        separator = " ")
public class DynamicConnectionManagerTest implements Callable<Void> {

    private static final Logger LOGGER = LoggerFactory.getLogger(DynamicConnectionManagerTest.class);

    private static final int DEFAULT_SERVER_PORT = 2998;
    private static final int DEFAULT_BUFFER_SIZE = 64;
    private static final int DEFAULT_DEVICE_ID = 0;

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

        var manager = new DynamicConnectionManager(port);

        TimeUnit.SECONDS.sleep(2);

        var buffer = manager.allocRegisteredBuffer(DEFAULT_DEVICE_ID, DEFAULT_BUFFER_SIZE);

        var string = new NativeString(buffer, 0, DEFAULT_BUFFER_SIZE);
        string.set("Hello from node " + manager.getLocalId());

        var remoteLids = manager.getRemoteLocalIds();
        var workloads = new WorkloadExecutor[remoteLids.length];

        for(int i = 0; i< remoteLids.length; i++) {
            workloads[i] = new WorkloadExecutor(manager, buffer, 0, remoteLids[i]);
            workloads[i].start();
        }

        TimeUnit.SECONDS.sleep(4);

        for(int i = 0; i< remoteLids.length; i++) {
            workloads[i].stop();
        }

        manager.shutdown();

        buffer.close();

        return null;

    }

    private class WorkloadExecutor extends Thread {
        private DynamicConnectionManager manager;
        private RegisteredBuffer data;
        private long offset;
        private short remoteLocalId;

        public WorkloadExecutor(DynamicConnectionManager manager, RegisteredBuffer data, long offset, short remoteLocalId) {
            this.manager = manager;
            this.data = data;
            this.offset = offset;
            this.remoteLocalId = remoteLocalId;
        }

        @Override
        public void run() {
            manager.remoteWrite(data, offset, DEFAULT_BUFFER_SIZE, remoteLocalId);
            LOGGER.info("Remote write complete");
        }
    }
}
