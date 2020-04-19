package de.hhu.bsinfo.neutrino.connection.message;

public enum MessageType {
    COMMON,
    HANDLER_REQ_CONNECTION,
    HANDLER_REQ_DISCONNECT,
    HANDLER_SEND_BUFFER_INFO,
    HANDLER_RESP_BUFFER_ACK,
    RC_CONNECT,
    RC_DISCONNECT
}
