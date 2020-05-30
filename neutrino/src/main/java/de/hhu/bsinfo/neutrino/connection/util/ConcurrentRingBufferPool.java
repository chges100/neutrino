package de.hhu.bsinfo.neutrino.connection.util;

import de.hhu.bsinfo.neutrino.util.Pool;
import de.hhu.bsinfo.neutrino.util.Poolable;

import java.util.Objects;
import java.util.function.Supplier;

/**
 * A concurrent ringbuffer pool based on the ringbuffer pool in neutrino
 *
 * @param <T> type the pool should store
 * @author Christian Gesse
 */
public class ConcurrentRingBufferPool<T extends Poolable> extends Pool<T> {

    /**
     * Concurrent ring buffer to store objects
     */
    private final ConcurrentRingBuffer<T> buffer;

    /**
     * Instantiates a new Concurrent ring buffer pool.
     *
     * @param size     the size of the pool
     * @param supplier the supplier for the pool
     */
    public ConcurrentRingBufferPool(final int size, final Supplier<T> supplier) {
        super(supplier);

        buffer = new ConcurrentRingBuffer<>(size);

        // fill buffer pool
        for (int i = 0; i < size; i++) {
            buffer.push(supplier.get());
        }
    }

    /**
     * Gets an instance of an object of type T
     *
     * @return instance of object of type T
     */
    @Override
    public final T getInstance() {
        // pop object from ring buffer or get new object from supplier
        return Objects.requireNonNullElseGet(buffer.pop(), getSupplier());
    }

    /**
     * Returns an instance of an object of type T into the pool
     *
     * @param instance instance of object of type T
     */
    @Override
    @SuppressWarnings("unchecked")
    public void returnInstance(Poolable instance) {
        // push object into pool ore release if pool is full
        if(!buffer.push((T) instance)) {
            instance.releaseInstance();
        }
    }
}