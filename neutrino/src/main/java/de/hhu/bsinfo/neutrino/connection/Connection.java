package de.hhu.bsinfo.neutrino.connection;

import de.hhu.bsinfo.neutrino.buffer.RegisteredBuffer;

import java.io.IOException;

public interface Connection {
    void init() throws IOException;
    void connect() throws IOException;
    void send(RegisteredBuffer data) throws IOException;
    void receive() throws IOException;
    void close() throws IOException;
}
