package de.hhu.bsinfo.neutrino.connection;

import de.hhu.bsinfo.neutrino.verbs.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Represents a socket for a queue pair and holds all important information regarding the queue pair.
 * Has to be extended for a specific queue pair type (reliable connection, unreliable datagram etc.).
 *
 * @author Christian Gesse
 */
public abstract class QPSocket {
    private static final Logger LOGGER = LoggerFactory.getLogger(QPSocket.class);

    /**
     * Minimum size of the send completion queue - the real size can be greater since.
     */
    protected int sendCompletionQueueSize = 100;

    /**
     * Minimum size of the receive completion queue size - the real size can be greater.
     */
    protected int receiveCompletionQueueSize = 100;

    /**
     * Size of the send queue.
     */
    protected int sendQueueSize = 100;

    /**
     * Size of the receive queue.
     */
    protected int receiveQueueSize = 100;

    /**
     * The device context.
     */
    private final DeviceContext deviceContext;

    /**
     * The Send completion queue.
     */
    protected final CompletionQueue sendCompletionQueue;
    /**
     * The Receive completion queue.
     */
    protected final CompletionQueue receiveCompletionQueue;
    /**
     * The Port attributes.
     */
    protected final PortAttributes portAttributes;

    /**
     * The Queue pair.
     */
    protected QueuePair queuePair;

    /**
     * The send queue fill count. Is used to track if another send work request can be posted or not to avoid
     * a send queue overflow.
     */
    protected final AtomicInteger sendQueueFillCount = new AtomicInteger(0);

    /**
     * The receive queue fill count. Is used to track if another send work request can be posted or not to avoid
     * a send queue overflow.
     */
    protected final AtomicInteger receiveQueueFillCount = new AtomicInteger(0);

    /**
     * Constructs a new Qp socket.
     *
     * @param deviceContext the device context
     * @throws IOException thrown if initialization failes
     */
    protected QPSocket(DeviceContext deviceContext) throws IOException {

        this.deviceContext = deviceContext;

        portAttributes = deviceContext.getContext().queryPort(1);
        if(portAttributes == null) {
            throw new IOException("Cannot query port");
        }

        sendCompletionQueue = deviceContext.getContext().createCompletionQueue(sendCompletionQueueSize);
        if(sendCompletionQueue == null) {
            throw new IOException("Cannot create completion queue");
        }

        receiveCompletionQueue = deviceContext.getContext().createCompletionQueue(receiveCompletionQueueSize);
        if(receiveCompletionQueue == null) {
            throw new IOException("Cannot create completion queue");
        }
    }

    /**
     * Instantiates a new Qp socket.
     *
     * @param deviceContext              the device context
     * @param sendQueueSize              the send queue size
     * @param receiveQueueSize           the receive queue size
     * @param sendCompletionQueueSize    the minimum send completion queue size
     * @param receiveCompletionQueueSize the minimum receive completion queue size
     * @throws IOException thrown if initialization failes
     */
    protected QPSocket(DeviceContext deviceContext, int sendQueueSize, int receiveQueueSize, int sendCompletionQueueSize, int receiveCompletionQueueSize) throws IOException {

        this.deviceContext = deviceContext;
        this.sendQueueSize = sendQueueSize;
        this.receiveQueueSize = receiveQueueSize;
        this.sendCompletionQueueSize = sendCompletionQueueSize;
        this.receiveCompletionQueueSize = receiveCompletionQueueSize;

        portAttributes = deviceContext.getContext().queryPort(1);
        if(portAttributes == null) {
            throw new IOException("Cannot query port");
        }

        sendCompletionQueue = deviceContext.getContext().createCompletionQueue(sendCompletionQueueSize);
        if(sendCompletionQueue == null) {
            throw new IOException("Cannot create completion queue");
        }

        receiveCompletionQueue = deviceContext.getContext().createCompletionQueue(receiveCompletionQueueSize);
        if(receiveCompletionQueue == null) {
            throw new IOException("Cannot create completion queue");
        }
    }

    /**
     * Instantiates a new Qp socket.
     *
     * @param deviceContext          the device context
     * @param sendQueueSize          the send queue size
     * @param receiveQueueSize       the receive queue size
     * @param sendCompletionQueue    the send completion queue
     * @param receiveCompletionQueue the receive completion queue
     * @throws IOException thrown if initialization failes
     */
    protected QPSocket(DeviceContext deviceContext, int sendQueueSize, int receiveQueueSize, CompletionQueue sendCompletionQueue, CompletionQueue receiveCompletionQueue) throws IOException {

        this.deviceContext = deviceContext;
        this.sendQueueSize = sendQueueSize;
        this.receiveQueueSize = receiveQueueSize;

        portAttributes = deviceContext.getContext().queryPort(1);
        if(portAttributes == null) {
            throw new IOException("Cannot query port");
        }

        this.sendCompletionQueue = sendCompletionQueue;
        this.receiveCompletionQueue = receiveCompletionQueue;

        this.sendCompletionQueueSize = sendCompletionQueue.getMaxElements();
        this.receiveCompletionQueueSize = receiveCompletionQueue.getMaxElements();
    }

    /**
     * Initialize the queue pair. Has to be implemented in deriving class.
     *
     * @throws IOException thrown if initialization failes
     */
    abstract void init() throws IOException;

    /**
     * Closes the queue pair. Has to be implemented in deriving class.
     *
     * @throws IOException thrown if initialization failes
     */
    abstract void close() throws IOException;

    /**
     * Post send work request to corresponding queue. If queue overflow would occur, try
     * again until successful.
     *
     * @param workRequest the send work request
     */
    protected void postSend(SendWorkRequest workRequest) {
        boolean posted = false;

        // try to post work request again if send queue is full
        while (!posted) {
            posted = tryPostSend(workRequest);
        }
    }

    /**
     * Post receive work request. to corresponding queue. If queue overflow would occur, try
     * again until successful.
     *
     * @param workRequest the work request
     */
    protected void postReceive(ReceiveWorkRequest workRequest) {
        boolean posted = false;

        // try to post work request again if send queue is full
        while (!posted) {
            posted = tryPostReceive(workRequest);
        }
    }

    /**
     * Try to post send work request. If send queue is full, the work request will not be posted.
     *
     * @param workRequest the work request
     * @return boolean indicating if work request was posted successfully or not
     */
    protected boolean tryPostSend(SendWorkRequest workRequest) {
        // get send queue fill count
        int oldVal = sendQueueFillCount.get();

        // check if send queue if full
        if(oldVal < sendQueueSize) {
            // increase send queue fill count
            int newVal = oldVal + 1;
            // try atomic compare and set to increase fill count. If successful,
            // post the work request
            if(sendQueueFillCount.compareAndSet(oldVal, newVal)) {
                return queuePair.postSend(workRequest);
            }
        }

        return false;
    }

    /**
     * Try to post receive work request. If receive queue is full, the work request will not be posted.
     *
     * @param workRequest the work request
     * @return boolean indicating if work request was posted successfully or not
     */
    protected boolean tryPostReceive(ReceiveWorkRequest workRequest) {
        // get receive queue fil count
        int oldVal = receiveQueueFillCount.get();

        // check if receive queue is full
        if(oldVal < receiveQueueSize) {
            int newVal = oldVal + 1;
            // try atomic compare and set to increase fill count. If successful,
            // post the work request
            if(receiveQueueFillCount.compareAndSet(oldVal, newVal)) {
                return queuePair.postReceive(workRequest);
            }
        }

        return false;
    }

    /**
     * Returns device context.
     *
     * @return the device context
     */
    public DeviceContext getDeviceContext() {
        return deviceContext;
    }

    /**
     * Returns port attributes.
     *
     * @return the port attributes
     */
    public PortAttributes getPortAttributes() {
        return portAttributes;
    }

    /**
     * Gets queue pair.
     *
     * @return the queue pair
     */
    public QueuePair getQueuePair() {
        return queuePair;
    }

    /**
     * Acknowledge send completion and decrease send queue fill count.
     */
    public void acknowledgeSendCompletion() {
        sendQueueFillCount.decrementAndGet();
    }

    /**
     * Acknowledge receive completion and decrease receive fill count.
     */
    public void acknowledgeReceiveCompletion() {
        receiveQueueFillCount.decrementAndGet();
    }

    /**
     * Returns send queue size.
     *
     * @return the send queue size
     */
    public int getSendQueueSize() {
        return sendQueueSize;
    }

    /**
     * Returns receive queue size.
     *
     * @return the receive queue size
     */
    public int getReceiveQueueSize() {
        return receiveQueueSize;
    }

    /**
     * Returns the send completion queue.
     * @return the send completion queue
     */
    public CompletionQueue getSendCompletionQueue() {
        return sendCompletionQueue;
    }

    /**
     * Returns the receive completion queue.
     * @return the receive completion queue
     */
    public CompletionQueue getReceiveCompletionQueue() {
        return receiveCompletionQueue;
    }
}