package de.hhu.bsinfo.neutrino.connection.interfaces;

import java.io.IOException;

public interface Connectable<T> {
    boolean connect(T remoteInfo) throws IOException;
    boolean disconnect() throws IOException;
}
