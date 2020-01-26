package de.hhu.bsinfo.neutrino.connection.message;

import de.hhu.bsinfo.neutrino.buffer.RegisteredBuffer;
import de.hhu.bsinfo.neutrino.connection.StaticConnectionManager;
import de.hhu.bsinfo.neutrino.connection.DeviceContext;
import de.hhu.bsinfo.neutrino.data.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Message implements NativeObject {

    private static final Logger LOGGER = LoggerFactory.getLogger(Message.class);

    private final NativeInteger messageType;
    private final NativeString payload;
    private final long handle;

    private final RegisteredBuffer byteBuffer;

    private static final int SIZE = 1024;

    public Message(DeviceContext deviceContext, MessageType messageType, String payload) {

        byteBuffer = StaticConnectionManager.allocLocalBuffer(deviceContext, SIZE);

        this.handle = byteBuffer.getHandle();

        this.messageType = new NativeInteger(byteBuffer, 0);
        this.messageType.set(messageType.ordinal());

        this.payload = new NativeString(byteBuffer, 4, SIZE - Integer.BYTES);
        this.payload.set(payload);
    }

    public Message(RegisteredBuffer byteBuffer) {

        this.byteBuffer = byteBuffer;

        this.handle = byteBuffer.getHandle();

        this.messageType = new NativeInteger(byteBuffer, 0);
        this.payload = new NativeString(byteBuffer, Integer.BYTES, SIZE - Integer.BYTES);
    }

    public Message(RegisteredBuffer byteBuffer, int offset) {
        this.byteBuffer = byteBuffer;

        this.handle = byteBuffer.getHandle();

        this.messageType = new NativeInteger(byteBuffer, offset);
        this.payload = new NativeString(byteBuffer, Integer.BYTES + offset, SIZE - Integer.BYTES);
    }

    public void setPayload(String payload) {
        this.payload.set(payload);
    }

    public String getPayload() {
        return payload.get();
    }

    public void setMessageType(int messageType) {
        this.messageType.set(messageType);
    }

    public MessageType getMessageType() {
        return MessageType.values()[messageType.get()];
    }

    public RegisteredBuffer getByteBuffer() {
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
        return "Message {\n" +
                "\tmessageType=" + MessageType.values()[messageType.get()] +
                "\n\tpayload=" + payload.get() +
                "\n}";
    }
}
