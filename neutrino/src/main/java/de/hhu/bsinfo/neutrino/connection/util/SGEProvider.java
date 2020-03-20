package de.hhu.bsinfo.neutrino.connection.util;

import de.hhu.bsinfo.neutrino.buffer.RegisteredBuffer;
import de.hhu.bsinfo.neutrino.connection.DeviceContext;
import de.hhu.bsinfo.neutrino.connection.dynamic.DynamicConnectionManager;
import de.hhu.bsinfo.neutrino.util.NativeObjectRegistry;
import de.hhu.bsinfo.neutrino.verbs.ScatterGatherElement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentLinkedQueue;

public class SGEProvider {
    private static final Logger LOGGER = LoggerFactory.getLogger(SGEProvider.class);


    private final DeviceContext deviceContext;
    private final RegisteredBuffer buffer;
    protected final int cnt;
    protected final int bufferSize;
    protected final ConcurrentLinkedQueue<ScatterGatherElement> scatterGatherElements;

    public SGEProvider(DeviceContext deviceContext, int cnt, int bufferSize) {
        this.cnt = cnt;
        this.bufferSize = bufferSize;
        this.deviceContext = deviceContext;

        buffer = deviceContext.allocRegisteredBuffer((long) cnt * bufferSize);

        scatterGatherElements = new ConcurrentLinkedQueue<>();

        for(int i = 0; i < cnt; i++) {
            var sge = new ScatterGatherElement(buffer.getHandle() + (long) i * bufferSize, bufferSize, buffer.getLocalKey());
            scatterGatherElements.add(sge);
            NativeObjectRegistry.registerObject(sge);
        }
    }

    public ScatterGatherElement getSGE() {
        return scatterGatherElements.poll();
    }
    public void returnSGE(ScatterGatherElement sge) {
        scatterGatherElements.offer(sge);
    }
}
