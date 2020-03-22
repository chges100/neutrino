package de.hhu.bsinfo.neutrino.connection.interfaces;

import java.io.IOException;

public interface Connectable<R, T> {
    R connect(T remoteInfo) throws IOException;
    void disconnect() throws IOException;
}
