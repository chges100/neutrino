package de.hhu.bsinfo.neutrino.connection.interfaces;

import java.io.IOException;

public interface Connectable<T> {
    void connect(T remoteInfo) throws IOException;
    void disconnect() throws IOException;
}
