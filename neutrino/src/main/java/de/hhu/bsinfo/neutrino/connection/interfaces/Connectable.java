package de.hhu.bsinfo.neutrino.connection.interfaces;

import de.hhu.bsinfo.neutrino.connection.util.ConnectionInformation;

import java.io.IOException;

public interface Connectable {
    void connect(ConnectionInformation remoteInfo) throws IOException;
}
