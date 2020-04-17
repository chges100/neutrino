package de.hhu.bsinfo.neutrino.connection.message;

import de.hhu.bsinfo.neutrino.buffer.RegisteredBuffer;
import de.hhu.bsinfo.neutrino.connection.DeviceContext;

public class Message extends BaseMessage<RegisteredBuffer> {
    public Message(DeviceContext deviceContext, MessageType messageType, long id, long ... payload) {
        super(deviceContext.allocRegisteredBuffer(BaseMessage.getSize()), messageType, id, payload);
    }

    public Message(RegisteredBuffer byteBuffer) {
        super(byteBuffer);
    }

    public void close() {
        getByteBuffer().close();
    }
}
