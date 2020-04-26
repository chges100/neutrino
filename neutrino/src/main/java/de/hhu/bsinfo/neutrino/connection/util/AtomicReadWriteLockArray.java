package de.hhu.bsinfo.neutrino.connection.util;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerArray;

public class AtomicReadWriteLockArray {
    private final AtomicIntegerArray array;
    private final int size;

    public AtomicReadWriteLockArray(int size) {
        array = new AtomicIntegerArray(size);
        this.size = size;
    }

    public int getLength() {
        return size;
    }

    public boolean tryReadLock(int i) {
        boolean ret = false;

        var oldValue = array.get(i);
        if(oldValue >= 0) {
            var newValue = oldValue + 1;
            ret = array.compareAndSet(i, oldValue, newValue);
        }

        return ret;
    }

    public void readLock(int i) {
        boolean locked = false;

        do {
            locked = tryReadLock(i);
        } while (!locked);
    }

    public boolean readLock(int i, long timeoutMs) {
        boolean locked = false;

        var start = System.nanoTime();
        var timeoutNs = TimeUnit.NANOSECONDS.convert(timeoutMs, TimeUnit.MILLISECONDS);

        do {
            locked = tryReadLock(i);
        } while (!locked && System.nanoTime() - start < timeoutNs);

        return locked;
    }

    public void unlockRead(int i) {
        var oldValue = array.getAndDecrement(i);

        if(oldValue <= 0) {
            throw new IllegalReadWriteLockState("Error unlocking: Lock was not read locked before");
        }
    }

    public boolean tryWriteLock(int i) {
        return array.compareAndSet(i, 0, -1);
    }

    public void writeLock(int i) {
        boolean locked = false;

        do {
            locked = tryWriteLock(i);
        } while (!locked);
    }

    public boolean writeLock(int i, long timeoutMs) {
        boolean locked = false;

        var start = System.nanoTime();
        var timeoutNs = TimeUnit.NANOSECONDS.convert(timeoutMs, TimeUnit.MILLISECONDS);

        do {
            locked = tryWriteLock(i);
        } while (!locked && System.nanoTime() - start < timeoutNs);

        return locked;
    }

    public void unlockWrite(int i) {
        var oldValue = array.getAndSet(i, 0);

        if(oldValue != -1) {
            throw  new IllegalReadWriteLockState("Error unlocking: Lock was not write locked before");
        }
    }

    public void convertWriteToReadLock(int i) {
        var oldValue = array.getAndSet(i, 1);

        if(oldValue != -1) {
            throw  new IllegalReadWriteLockState("Error converting write to read lock: Lock was not write locked before");
        }
    }

    public boolean isWriteLocked(int i) {
        return array.get(i) < 0;
    }

    public boolean isReadLocked(int i) {
        return array.get(i) > 0;
    }

    public class IllegalReadWriteLockState extends  RuntimeException {

        public IllegalReadWriteLockState(String message) {
            super(message);
        }
    }
}
