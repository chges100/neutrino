package de.hhu.bsinfo.neutrino.connection.message;

import de.hhu.bsinfo.neutrino.buffer.LocalBuffer;
import de.hhu.bsinfo.neutrino.data.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Base Type for a fixed size message object located in antive memory
 *
 * @param <T> native buffer type to use
 * @author Christian Gesse
 */
public class BaseMessage<T extends LocalBuffer> implements NativeObject {

    private static final Logger LOGGER = LoggerFactory.getLogger(BaseMessage.class);

    /**
     * Unique id provider for messages - 0 is an invalid id.
     */
    private static final AtomicLong globalIdProvider = new AtomicLong(1);

    /**
     * Size of message payload counted in longs (payload is created as array of longs)
     */
    private static final int PAYLOAD_COUNT = 4;

    /**
     * Fixed size of the message
     */
    private static final int SIZE = Integer.BYTES + Long.BYTES +  PAYLOAD_COUNT * Long.BYTES;

    /**
     * The type of the message
     */
    private final NativeInteger messageType;
    /**
     * The unique id of the message
     */
    private final NativeLong id;
    /**
     * The payload of the message represented as a fixed size array of longs
     */
    private final NativeLong[] payload;
    /**
     * The handle of the message given as the buffer address in native memory
     */
    private final long handle;

    /**
     * The byte buffer containing the message
     */
    private final T byteBuffer;


    /**
     * Constructor for a message using a preallocated buffer.
     *
     * @param byteBuffer the native buffer to use
     * @param messageType the type of the message
     * @param id the unique id of the message
     * @param payload payload of the message represeted as array of longs
     */
    public BaseMessage(T byteBuffer, MessageType messageType, long id, long ... payload) {

        // set buffer information and clear memory
        this.byteBuffer = byteBuffer;
        this.handle = byteBuffer.getHandle();
        this.byteBuffer.clear();

        // set the message informations
        this.messageType = new NativeInteger(byteBuffer, 0);
        this.messageType.set(messageType.ordinal());

        this.id = new NativeLong(byteBuffer, Integer.BYTES);
        this.id.set(id);

        var varCount = payload.length;

        this.payload = new NativeLong[PAYLOAD_COUNT];

        // copy payload into message buffer
        for(int i = 0; i < PAYLOAD_COUNT; i++) {
            this.payload[i] = new NativeLong(byteBuffer, Integer.BYTES + Long.BYTES + i * Long.BYTES);

            if(i < varCount) {
                this.payload[i].set(payload[i]);
            } else {
                this.payload[i].set(0);
            }
        }
    }

    /**
     * Wraps a new message from a preallocated native buffer filled with data.
     *
     * @param byteBuffer the buffer with message data
     */
    public BaseMessage(T byteBuffer) {

        // set buffer information
        this.byteBuffer = byteBuffer;
        this.handle = byteBuffer.getHandle();

        // wrap buffer's content into message
        this.messageType = new NativeInteger(byteBuffer, 0);
        this.id = new NativeLong(byteBuffer, Integer.BYTES);
        this.payload = new NativeLong[PAYLOAD_COUNT];

        for(int i = 0; i < PAYLOAD_COUNT; i++) {
            this.payload[i] = new NativeLong(byteBuffer, Integer.BYTES + Long.BYTES + i * Long.BYTES);
        }
    }

    /**
     * Wraps a new message from a preallocated native buffer filled with data beginning from an offset.
     * One example use if for received datagrams via unreliable datagram  - the message data is placed behind a routing information,
     * so that the offset is necessary.
     *
     * @param byteBuffer the buffer with message data
     */
    public BaseMessage(T byteBuffer, int offset) {

        // set buffer information
        this.byteBuffer = byteBuffer;
        this.handle = byteBuffer.getHandle();

        // wrap buffer's content into message beginning from an offset
        this.messageType = new NativeInteger(byteBuffer, offset);
        this.id = new NativeLong(byteBuffer, offset + Integer.BYTES);
        this.payload = new NativeLong[PAYLOAD_COUNT];

        for(int i = 0; i < PAYLOAD_COUNT; i++) {
            this.payload[i] = new NativeLong(byteBuffer, offset + Integer.BYTES + Long.BYTES + i * Long.BYTES);
        }
    }

    /**
     * Sets the payload of the message
     *
     * @param payload payload given as a long array
     */
    public void setPayload(long ... payload) {
        var varCount = payload.length;

        for(int i = 0; i < PAYLOAD_COUNT; i++) {
            if(i < varCount) {
                this.payload[i].set(payload[i]);
            } else {
                this.payload[i].set(0);
            }
        }
    }

    /**
     * Returns the payload of the message
     *
     * @return payload of the message as an array of longs
     */
    public long[] getPayload() {
        var ret = new long[PAYLOAD_COUNT];

        for(int i = 0; i < PAYLOAD_COUNT; i++) {
            ret[i] = this.payload[i].get();
        }

        return ret;
    }

    /**
     * Sets the type of the message based on an integer value
     *
     * @param messageType message type as integer
     */
    public void setMessageType(int messageType) {
        this.messageType.set(messageType);
    }

    /**
     * Sets the type of the message based on the message type enum
     *
     * @param messageType message type as entry of enum
     */
    public void setMessageType(MessageType messageType) {
        this.messageType.set(messageType.ordinal());
    }

    /**
     * Gets the type of the message
     *
     * @return the message type
     */
    public MessageType getMessageType() {
        return MessageType.values()[messageType.get()];
    }

    /**
     * Gets the byte buffer containing the message
     *
     * @return the byte buffer containing the message
     */
    public T getByteBuffer() {
        return byteBuffer;
    }

    /**
     * Gets the size of the message
     *
     * @return size of the message in bytes
     */
    public static int getSize() {
        return SIZE;
    }

    public void setId(long id) {
        this.id.set(id);
    }

    /**
     * Gets the id of the message
     * @return unique id of the message
     */
    public long getId() {
        return id.get();
    }

    /**
     * Provides a global id for the message on this node
     * @return the id
     */
    public static long provideGlobalId() {
        return globalIdProvider.getAndIncrement();
    }

    /**
     * Gets the handle (the native buffer address) of the message
     *
     * @return the handle
     */
    @Override
    public long getHandle() {
        return handle;
    }

    /**
     * Gets the size of the message as an native object.
     * Is identical to the message size.
     *
     * @return native size of the message
     */
    @Override
    public long getNativeSize() {
        return SIZE;
    }

    /**
     * Returns  message data as a string
     * @return message data as a string
     */
    @Override
    public String toString() {

        var ret = "Message {\n" +
                "\tmessageType=" + MessageType.values()[messageType.get()] + "\n";

        ret += "\tId=" + id.get() + "\n";

        for(int i = 0; i < PAYLOAD_COUNT; i++) {
            ret += "Payload " + i + ": " + this.payload[i].get() + "\n";
        }

        return ret;
    }
}
