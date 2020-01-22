package de.hhu.bsinfo.neutrino.connection.interfaces;

import de.hhu.bsinfo.neutrino.connection.ConnectionInformation;

import java.io.IOException;
import java.net.Socket;

public interface Connectable {
    void connect(ConnectionInformation remoteInfo) throws IOException;
}
