package de.hhu.bsinfo.neutrino.connection;

import de.hhu.bsinfo.neutrino.buffer.RegisteredBuffer;
import de.hhu.bsinfo.neutrino.verbs.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;

public class RDMAConnection  extends ReliableConnection{
    private static final Logger LOGGER = LoggerFactory.getLogger(RDMAConnection.class);

    private static final ConnectionType connectionType = ConnectionType.RDMAConnection;

    public RDMAConnection(DeviceContext deviceContext) throws IOException {
        super(deviceContext);
    }

    public static ConnectionType getConnectionType() {
        return connectionType;
    }
}

