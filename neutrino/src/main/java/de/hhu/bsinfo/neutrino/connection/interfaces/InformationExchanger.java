package de.hhu.bsinfo.neutrino.connection.interfaces;

import java.io.IOException;

public interface InformationExchanger<T> {
    T getRemoteInformation() throws IOException;
    void sendLocalInformation() throws IOException;
    T exchangeInformation() throws IOException;
}
