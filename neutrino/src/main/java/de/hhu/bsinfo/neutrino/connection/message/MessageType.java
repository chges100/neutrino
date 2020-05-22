package de.hhu.bsinfo.neutrino.connection.message;

/**
 * Enum describing the type of a message.
 *
 * @author Christian Gesse
 */
public enum MessageType {
    /**
     * Common message type for test messages
     */
    COMMON,
    /**
     * Type for a connection request used in dynamic connection handler
     */
    HANDLER_REQ_CONNECTION,
    /**
     * Type for a disconnect request used in dynamic connection handler
     */
    HANDLER_REQ_DISCONNECT,
    /**
     * Type for a disconnect force messsage used in dynamic connection handler
     */
    HANDLER_SEND_DISCONNECT_FORCE,
    /**
     * Type for a buffer information message used in dynamic connection handler
     */
    HANDLER_SEND_BUFFER_INFO,
    /**
     * Type for a buffer information acknowledged message used in dynamic connection handler
     */
    HANDLER_RESP_BUFFER_ACK,
    /**
     * Type for a response to a connection request used in dynamic connection handler
     */
    HANDLER_RESP_CONNECTION_REQ,
    /**
     * Type for connection request message used in handshake of reliable connection
     */
    RC_CONNECT,
    /**
     * Type for disconnect request message used in handshake of reliable connection
     */
    RC_DISCONNECT
}
