package de.hhu.bsinfo.neutrino.connection.message;

import de.hhu.bsinfo.neutrino.buffer.RegisteredBuffer;
import de.hhu.bsinfo.neutrino.connection.DeviceContext;

public class Message extends BaseMessage<RegisteredBuffer> {
    public Message(DeviceContext deviceContext, MessageType messageType, String payload) {
        super(deviceContext.allocLocalBuffer(BaseMessage.getSize()), messageType, payload);
    }

    public Message(RegisteredBuffer byteBuffer) {
        super(byteBuffer);
    }
}
