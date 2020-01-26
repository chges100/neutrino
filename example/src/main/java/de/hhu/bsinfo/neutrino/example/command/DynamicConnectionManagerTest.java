package de.hhu.bsinfo.neutrino.example.command;

import de.hhu.bsinfo.neutrino.buffer.BufferInformation;
import de.hhu.bsinfo.neutrino.connection.StaticConnectionManager;
import de.hhu.bsinfo.neutrino.connection.UnreliableDatagram;
import de.hhu.bsinfo.neutrino.connection.dynamic.DynamicConnectionManager;
import de.hhu.bsinfo.neutrino.connection.message.Message;
import de.hhu.bsinfo.neutrino.connection.message.MessageType;
import de.hhu.bsinfo.neutrino.connection.util.SocketUDInformationExchanger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.sql.Time;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

@CommandLine.Command(
        name = "dyn-con-test",
        description = "Starts a simple test to see if the dynamic connection manager works properly.%n",
        showDefaultValues = true,
        separator = " ")
public class DynamicConnectionManagerTest implements Callable<Void> {

    private static final Logger LOGGER = LoggerFactory.getLogger(DynamicConnectionManagerTest.class);

    private static final int DEFAULT_SERVER_PORT = 2998;
    private static final int DEFAULTBUFSIZE = 64;

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

        TimeUnit.SECONDS.sleep(4);

        manager.shutdown();

        manager.printRemoteInfos();

        return null;

    }
}
