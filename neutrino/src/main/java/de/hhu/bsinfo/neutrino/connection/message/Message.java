package de.hhu.bsinfo.neutrino.connection.message;

import de.hhu.bsinfo.neutrino.buffer.RegisteredBuffer;
import de.hhu.bsinfo.neutrino.connection.DeviceContext;

/**
 * Implementation of a message using a registered buffer (a memory region).
 * Since this buffer is pinned in virtual memory, it can be used for InfiniBand transmissions.
 *
 * @author Christian Gesse
 */
public class Message extends BaseMessage<RegisteredBuffer> {

    /**
     * Constructor for a message given a preallocated registered buffer
     *
     * @param byteBuffer the native registered buffer to use
     * @param messageType the type of the message
     * @param id the unique id of the message
     * @param payload payload of the message represeted as array of longs
     */
    public Message(DeviceContext deviceContext, MessageType messageType, long id, long ... payload) {
        super(deviceContext.allocRegisteredBuffer(BaseMessage.getSize()), messageType, id, payload);
    }

    /**
     * Wraps a new message from a preallocated native registered buffer filled with data.
     *
     * @param byteBuffer the registered buffer with message data
     */
    public Message(RegisteredBuffer byteBuffer) {
        super(byteBuffer);
    }

    /**
     * Wraps a new message from a preallocated native registered buffer filled with data beginning from an offset.
     * One example use if for received datagrams via unreliable datagram  - the message data is placed behind a routing information,
     * so that the offset is necessary.
     *
     * @param byteBuffer the registered buffer with message data
     */
    public void close() {
        getByteBuffer().close();
    }
}
