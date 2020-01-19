package de.hhu.bsinfo.neutrino.connection.interfaces;

import java.io.IOException;
import java.net.Socket;

public interface Connectable {
    void connect(Socket socket) throws IOException;
}
