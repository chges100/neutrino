package de.hhu.bsinfo.neutrino.connection.message;

import de.hhu.bsinfo.neutrino.buffer.LocalBuffer;
import de.hhu.bsinfo.neutrino.data.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BaseMessage<T extends LocalBuffer> implements NativeObject {

    private static final Logger LOGGER = LoggerFactory.getLogger(BaseMessage.class);
    private static final int PAYLOAD_COUNT = 4;
    private static final int SIZE = Integer.BYTES + PAYLOAD_COUNT * Long.BYTES;

    private final NativeInteger messageType;
    private final NativeLong[] payload;
    private final long handle;

    private final T byteBuffer;



    public BaseMessage(T byteBuffer, MessageType messageType, long ... payload) {

        this.byteBuffer = byteBuffer;

        this.handle = byteBuffer.getHandle();
        this.byteBuffer.clear();

        this.messageType = new NativeInteger(byteBuffer, 0);
        this.messageType.set(messageType.ordinal());

        var varCount = payload.length;

        this.payload = new NativeLong[PAYLOAD_COUNT];


        for(int i = 0; i < PAYLOAD_COUNT; i++) {
            this.payload[i] = new NativeLong(byteBuffer, Integer.BYTES + i * Long.BYTES);

            if(i < varCount) {
                this.payload[i].set(payload[i]);
            } else {
                this.payload[i].set(0);
            }
        }
    }

    public BaseMessage(T byteBuffer) {

        this.byteBuffer = byteBuffer;

        this.handle = byteBuffer.getHandle();

        this.messageType = new NativeInteger(byteBuffer, 0);

        this.payload = new NativeLong[PAYLOAD_COUNT];

        for(int i = 0; i < PAYLOAD_COUNT; i++) {
            this.payload[i] = new NativeLong(byteBuffer, Integer.BYTES + i * Long.BYTES);
        }
    }

    public BaseMessage(T byteBuffer, int offset) {
        this.byteBuffer = byteBuffer;

        this.handle = byteBuffer.getHandle();

        this.messageType = new NativeInteger(byteBuffer, offset);

        this.payload = new NativeLong[PAYLOAD_COUNT];

        for(int i = 0; i < PAYLOAD_COUNT; i++) {
            this.payload[i] = new NativeLong(byteBuffer, offset + Integer.BYTES + i * Long.BYTES);
        }
    }

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

    public long[] getPayload() {
        var ret = new long[PAYLOAD_COUNT];

        for(int i = 0; i < PAYLOAD_COUNT; i++) {
            ret[i] = this.payload[i].get();
        }

        return ret;
    }

    public void setMessageType(int messageType) {
        this.messageType.set(messageType);
    }

    public void setMessageType(MessageType messageType) {
        this.messageType.set(messageType.ordinal());
    }

    public MessageType getMessageType() {
        return MessageType.values()[messageType.get()];
    }

    public T getByteBuffer() {
        return byteBuffer;
    }

    public static int getSize() {
        return SIZE;
    }

    @Override
    public long getHandle() {
        return handle;
    }

    @Override
    public long getNativeSize() {
        return SIZE;
    }

    @Override
    public String toString() {

        var ret = "Message {\n" +
                "\tmessageType=" + MessageType.values()[messageType.get()] + "\n";

        for(int i = 0; i < PAYLOAD_COUNT; i++) {
            ret += "Payload " + i + ": " + this.payload[i].get() + "\n";
        }

        return ret;
    }
}
