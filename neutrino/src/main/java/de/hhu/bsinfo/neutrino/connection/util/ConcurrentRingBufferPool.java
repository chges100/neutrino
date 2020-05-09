package de.hhu.bsinfo.neutrino.connection.util;

import de.hhu.bsinfo.neutrino.util.Pool;
import de.hhu.bsinfo.neutrino.util.Poolable;

import java.util.Objects;
import java.util.function.Supplier;

public class ConcurrentRingBufferPool<T extends Poolable> extends Pool<T> {

    private final ConcurrentRingBuffer<T> buffer;

    public ConcurrentRingBufferPool(final int size, final Supplier<T> supplier) {
        super(supplier);

        buffer = new ConcurrentRingBuffer<>(size);

        for (int i = 0; i < size; i++) {
            buffer.push(supplier.get());
        }
    }

    @Override
    public final T getInstance() {
        return Objects.requireNonNullElseGet(buffer.pop(), getSupplier());
    }

    @Override
    @SuppressWarnings("unchecked")
    public void returnInstance(Poolable instance) {
        if(!buffer.push((T) instance)) {
            instance.releaseInstance();
        }
    }
}