package de.hhu.bsinfo.neutrino.connection.util;

import org.jctools.queues.MpmcArrayQueue;


import java.util.ArrayList;

/**
 * Simple concurrent ring buffer based on queue from JCTools and the ring buffer implementation in neutrino
 * Warning: The push function is only partly threadsafe!
 *
 * @param <T> type the ring buffer should store
 * @author Christian Gesse
 */
public class ConcurrentRingBuffer<T> {

    /**
     * The muliple producer multiple consumer structure that holds the buffer
     */
    private final MpmcArrayQueue<T> buffer;


    /**
     * Instantiates a new concurrent ring buffer.
     *
     * @param size the size of the buffer (= number of entries)
     */
    public ConcurrentRingBuffer(final int size) {
        buffer = new MpmcArrayQueue<>(size);
    }

    /**
     * Gets the size (number of entries) of ring buffer
     *
     * @return the int
     */
    public int size() {
        return buffer.size();
    }

    /**
     * Clears the ring buffer and returns content as an array
     *
     * @return array containing the content of the buffer
     */
    public ArrayList<T> clear() {
        var ret = new ArrayList<T>();
        buffer.removeAll(ret);

        return ret;
    }

    /**
     * Check if buffer is empty
     *
     * @return boolean indicating if buffer is empty
     */
    public boolean isEmpty() {
        return buffer.isEmpty();
    }

    /**
     * Check if buffer is full
     * (not threadsafe)
     *
     * @return boolean indicating if buffer is full
     */
    public boolean isFull() {
        return buffer.capacity() - buffer.size() == 0;
    }

    /**
     * Push new object into the buffer
     * Note: The pushing into the buffer itself is threadsafe, the checks around it are not!
     *
     * @param object the object to push
     * @return boolean if object was pushed
     */
    public boolean push(final T object) {
        if(!isFull()) {
            buffer.offer(object);

            return true;
        }

        return false;
    }

    /**
     * Pop a new object from the buffer
     *
     * @return the object
     */
    public T pop() {
        return buffer.poll();
    }
}
