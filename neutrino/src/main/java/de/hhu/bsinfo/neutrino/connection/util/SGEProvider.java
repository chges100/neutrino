package de.hhu.bsinfo.neutrino.connection.util;

import de.hhu.bsinfo.neutrino.buffer.RegisteredBuffer;
import de.hhu.bsinfo.neutrino.connection.DeviceContext;
import de.hhu.bsinfo.neutrino.connection.dynamic.DynamicConnectionManager;
import de.hhu.bsinfo.neutrino.util.NativeObjectRegistry;
import de.hhu.bsinfo.neutrino.verbs.ScatterGatherElement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Provider for a number of scatter gather elements with identical properties.
 * All elements are referencing into one continous block of memory (that is allocated as an registered buffer).
 * Use case if for messaging where all messages have equal size and the buffers should be reused.
 *
 * @author Christian Gesse
 */
public class SGEProvider {
    private static final Logger LOGGER = LoggerFactory.getLogger(SGEProvider.class);

    /**
     * The device context that is used to allocate the buffer
     */
    private final DeviceContext deviceContext;
    /**
     * Registered buffer (memory region) the scatter gahter elements are referencing into
     */
    private final RegisteredBuffer buffer;
    /**
     * The count of scatter gather elements this provider can provide
     */
    protected final int cnt;
    /**
     * Size of one block in the buffer a scatter gather element is referencing to
     */
    protected final int bufferSize;

    /**
     * The Scatter gather elements stored in a queue
     */
    protected final ConcurrentLinkedQueue<ScatterGatherElement> scatterGatherElements;

    /**
     * Instantiates a new Sge provider.
     *
     * @param deviceContext the device context
     * @param cnt           the count of scatter gather elements
     * @param bufferSize    the buffer size for each block a sge is referencing to
     */
    public SGEProvider(DeviceContext deviceContext, int cnt, int bufferSize) {
        this.cnt = cnt;
        this.bufferSize = bufferSize;
        this.deviceContext = deviceContext;

        // Get continous block of memory. This block is divided into cnt parts
        buffer = deviceContext.allocRegisteredBuffer((long) cnt * bufferSize);

        scatterGatherElements = new ConcurrentLinkedQueue<>();

        // create all scatter gather elements and fill up queue
        for(int i = 0; i < cnt; i++) {
            var sge = new ScatterGatherElement(buffer.getHandle() + (long) i * bufferSize, bufferSize, buffer.getLocalKey());
            scatterGatherElements.add(sge);
            NativeObjectRegistry.registerObject(sge);
        }
    }

    /**
     * Gets an scatter gather element
     *
     * @return the sge or null if no sge is available
     */
    public ScatterGatherElement getSGE() {
        return scatterGatherElements.poll();
    }

    /**
     * Return an scatter gather element into the provider
     *
     * @param sge the sge
     */
    public void returnSGE(ScatterGatherElement sge) {
        scatterGatherElements.offer(sge);
    }
}
