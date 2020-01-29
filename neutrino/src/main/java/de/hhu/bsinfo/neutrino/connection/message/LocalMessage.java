package de.hhu.bsinfo.neutrino.connection.message;

import de.hhu.bsinfo.neutrino.buffer.LocalBuffer;
import de.hhu.bsinfo.neutrino.buffer.RegisteredBuffer;
import de.hhu.bsinfo.neutrino.connection.DeviceContext;

public class LocalMessage extends BaseMessage<LocalBuffer> {
    public LocalMessage(LocalBuffer byteBuffer, MessageType messageType, String payload) {
        super(byteBuffer, messageType, payload);
    }

    public LocalMessage(LocalBuffer byteBuffer) {
        super(byteBuffer);
    }

    public LocalMessage(LocalBuffer byteBuffer, int offset) {
        super(byteBuffer, offset);
    }
}
