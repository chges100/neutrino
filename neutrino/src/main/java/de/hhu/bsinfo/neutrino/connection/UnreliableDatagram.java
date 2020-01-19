package de.hhu.bsinfo.neutrino.connection;

import de.hhu.bsinfo.neutrino.verbs.QueuePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class UnreliableDatagram extends QPSocket{
    private static final Logger LOGGER = LoggerFactory.getLogger(UnreliableDatagram.class);

    private final QueuePair queuePair;


    public UnreliableDatagram(DeviceContext deviceContext) throws IOException {

        super(deviceContext);

        queuePair = deviceContext.getProtectionDomain().createQueuePair(new QueuePair.InitialAttributes.Builder(
                QueuePair.Type.UD, sendCompletionQueue, receiveCompletionQueue, sendQueueSize, receiveQueueSize, 1, 1).build());
        if(queuePair == null) {
            throw new IOException("Cannot create queue pair");
        }
    }

    @Override
    public void init() throws IOException {
        if(!queuePair.modify(QueuePair.Attributes.Builder.buildInitAttributesUD((short) 0, (byte) 1))) {
            throw new IOException("Unable to move queue pair into INIT state");
        }
    }

    @Override
    public void close() {
        queuePair.close();
    }
}
