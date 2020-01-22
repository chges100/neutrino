package de.hhu.bsinfo.neutrino.connection.interfaces;

import de.hhu.bsinfo.neutrino.connection.ConnectionInformation;

import java.io.IOException;

public interface Connector {
    ConnectionInformation getConnectionInformation() throws IOException;
    void connect() throws IOException;
}
