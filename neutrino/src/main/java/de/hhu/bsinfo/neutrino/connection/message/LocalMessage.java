package de.hhu.bsinfo.neutrino.connection.message;

import de.hhu.bsinfo.neutrino.buffer.LocalBuffer;
import de.hhu.bsinfo.neutrino.buffer.RegisteredBuffer;
import de.hhu.bsinfo.neutrino.connection.DeviceContext;

/**
 * Implementation of a message as a local object (not sendable via InfiniBand) in native buffer.
 *
 * @author Christian Gesse
 */
public class LocalMessage extends BaseMessage<LocalBuffer> {

    /**
     * Constructor for a local message given a preallocated local buffer
     *
     * @param byteBuffer the native local buffer to use
     * @param messageType the type of the message
     * @param id the unique id of the message
     * @param payload payload of the message represeted as array of longs
     */
    public LocalMessage(LocalBuffer byteBuffer, MessageType messageType, long id, long ... payload) {
        super(byteBuffer, messageType, id, payload);
    }

    /**
     * Wraps a new message from a preallocated native local buffer filled with data.
     *
     * @param byteBuffer the local buffer with message data
     */
    public LocalMessage(LocalBuffer byteBuffer) {
        super(byteBuffer);
    }

    /**
     * Wraps a new message from a preallocated native local buffer filled with data beginning from an offset.
     * One example use if for received datagrams via unreliable datagram  - the message data is placed behind a routing information,
     * so that the offset is necessary.
     *
     * @param byteBuffer the local buffer with message data
     */
    public LocalMessage(LocalBuffer byteBuffer, int offset) {
        super(byteBuffer, offset);
    }
}
