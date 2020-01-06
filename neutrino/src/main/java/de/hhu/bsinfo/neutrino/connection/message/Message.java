package de.hhu.bsinfo.neutrino.connection.message;

import de.hhu.bsinfo.neutrino.buffer.LocalBuffer;
import de.hhu.bsinfo.neutrino.buffer.RegisteredBuffer;
import de.hhu.bsinfo.neutrino.connection.Connection;
import de.hhu.bsinfo.neutrino.connection.ConnectionManager;
import de.hhu.bsinfo.neutrino.connection.DeviceContext;
import de.hhu.bsinfo.neutrino.data.*;
import de.hhu.bsinfo.neutrino.util.Poolable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class Message implements NativeObject {

    private static final Logger LOGGER = LoggerFactory.getLogger(Message.class);

    private final NativeInteger messageType;
    private final NativeInteger payloadSize;
    private final NativeString payload;
    private final long handle;

    private final RegisteredBuffer byteBuffer;

    private int SIZE;

    private final Connection connection;

    public Message(Connection connection, MessageType messageType, String payload) {
        this.connection = connection;

        SIZE = Integer.BYTES + Integer.BYTES + payload.length();
        byteBuffer = ConnectionManager.allocLocalBuffer(connection.getDeviceContext(), SIZE);

        this.handle = byteBuffer.getHandle();

        this.messageType = new NativeInteger(byteBuffer, 0);
        this.messageType.set(messageType.ordinal());

        this.payloadSize = new NativeInteger(byteBuffer, 4);
        this.payloadSize.set(payload.length());

        this.payload = new NativeString(byteBuffer, 8, payloadSize.get());
        this.payload.set(payload);


        LOGGER.info("Payload: {}", this.payload);
    }

    public Message(Connection connection, RegisteredBuffer byteBuffer) {
        this.connection = connection;

        this.byteBuffer = byteBuffer;

        this.handle = byteBuffer.getHandle();

        this.messageType = new NativeInteger(byteBuffer, 0);
        this.payloadSize = new NativeInteger(byteBuffer, 4);
        this.payload = new NativeString(byteBuffer, 8, payloadSize.get());
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

    public int getPayloadSize() {
        return payloadSize.get();
    }

    public RegisteredBuffer getByteBuffer() {
        return byteBuffer;
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
                "\n\tpayloadSize=" + payloadSize.get() +
                "\n\tpayload=" + payload.get() +
                "\n}";
    }

    public static int getMessageSizeForPayload(int payloadSize) {
        return Integer.BYTES + Integer.BYTES + payloadSize;
    }

}
