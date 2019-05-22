package de.hhu.bsinfo.neutrino.benchmark.pool;

import de.hhu.bsinfo.neutrino.data.NativeObject;
import de.hhu.bsinfo.neutrino.util.NativeObjectFactory;
import java.util.Objects;
import java.util.function.Supplier;

public class QueuePool<T extends NativeObject> extends QueueStore<T> implements NativeObjectFactory<T> {

    private final Supplier<T> supplier;

    public QueuePool(final QueueType type, final int size, final Supplier<T> supplier) {
        super(type, size);

        this.supplier = supplier;

        for(int i = 0; i < size; i++) {
            storeInstance(supplier.get());
        }
    }

    @Override
    public T newInstance() {
        return Objects.requireNonNullElseGet(getInstance(), supplier);
    }
}
