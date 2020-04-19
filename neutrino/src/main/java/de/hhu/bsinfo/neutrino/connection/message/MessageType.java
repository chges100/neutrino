package de.hhu.bsinfo.neutrino.connection.message;

public enum MessageType {
    // just for test messages
    COMMON,
    // Message Types used by DynamicConnectionHandler
    HANDLER_REQ_CONNECTION,
    HANDLER_REQ_DISCONNECT,
    HANDLER_SEND_DISCONNECT_FORCE,
    HANDLER_SEND_BUFFER_INFO,
    HANDLER_RESP_BUFFER_ACK,
    // Message Types used by ReliableConnection
    RC_CONNECT,
    RC_DISCONNECT
}
