package de.hhu.bsinfo.neutrino.connection.util;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerArray;


/**
 * An memory efficient array of simple read write locks based on an atomic integer array.
 * Can be used to provide a high number of read write locks without need to instanciate each lock seperately.
 *
 * @author Christian Gesse
 */
public class AtomicReadWriteLockArray {

    /**
     * The atomic integer array representing the state of each lock:
     * 0 -> not locked
     * N > 0 -> read locked N times
     * -1 -> write locked exclusively
     */
    private final AtomicIntegerArray array;
    private final int size;

    /**
     * Instantiates a new Atomic read write lock array.
     *
     * @param size size of the lock array
     */
    public AtomicReadWriteLockArray(int size) {
        array = new AtomicIntegerArray(size);
        this.size = size;
    }

    /**
     * Gets the size of the lock array
     *
     * @return the length of the lock array
     */
    public int getLength() {
        return size;
    }

    /**
     * Tries to obtain a read lock for position i.
     *
     * @param i the position that should be read locked
     * @return boolean indicating if read lock was obtained
     */
    public boolean tryReadLock(int i) {
        boolean ret = false;

        // get old value, check if read locking is possible and use compare and set
        var oldValue = array.get(i);
        if(oldValue >= 0) {
            var newValue = oldValue + 1;
            ret = array.compareAndSet(i, oldValue, newValue);
        }

        return ret;
    }

    /**
     * Blocks until read lock is obtained.
     *
     * @param i position that should be read locked
     */
    public void readLock(int i) {
        boolean locked = false;

        // try to obtain a read lock until successful
        do {
            locked = tryReadLock(i);
        } while (!locked);
    }


    /**
     * Blocks until a given timeout is reached or a read lock is obtained
     *
     * @param i         position that should be read locked
     * @param timeoutMs the timeout to block in ms
     * @return boolean indicating if read lock was obtained
     */
    public boolean readLock(int i, long timeoutMs) {
        boolean locked = false;

        // get start time
        var start = System.nanoTime();
        var timeoutNs = TimeUnit.NANOSECONDS.convert(timeoutMs, TimeUnit.MILLISECONDS);

        // try to obtain read lock until timeout is reached
        do {
            locked = tryReadLock(i);
        } while (!locked && System.nanoTime() - start < timeoutNs);

        return locked;
    }

    /**
     * Releases a read lock
     *
     * @param i position where read lock should be released
     */
    public void unlockRead(int i) {
        // decrement number of obtained read locks
        var oldValue = array.getAndDecrement(i);

        // if the lock was not read locked before, an illegal state exception is thrown
        if(oldValue <= 0) {
            throw new IllegalReadWriteLockState("Error unlocking: Lock was not read locked before");
        }
    }

    /**
     * Try to obtain an exclusive write lock
     *
     * @param i the position that should be write locked
     * @return boolean indicating if lock was obtained
     */
    public boolean tryWriteLock(int i) {
        // since only one write lock can be obtained and only if no read lock is active,
        // it suffices to check if the current value is 0 and to replace it with -1
        return array.compareAndSet(i, 0, -1);
    }

    /**
     * Blocks until exclusive write lock is obtained
     *
     * @param i position that should be write locked
     */
    public void writeLock(int i) {
        boolean locked = false;

        // iterate until write lock is obtained
        do {
            locked = tryWriteLock(i);
        } while (!locked);
    }

    /**
     * Blocks until given timeout is reached or write lock is obtained
     *
     * @param i         position that should be write locked
     * @param timeoutMs the timeout to block in ms
     * @return boolean indicating if lock was obtained
     */
    public boolean writeLock(int i, long timeoutMs) {
        boolean locked = false;

        // get start time
        var start = System.nanoTime();
        var timeoutNs = TimeUnit.NANOSECONDS.convert(timeoutMs, TimeUnit.MILLISECONDS);

        // iterate until timeout is reached or lock is obtained
        do {
            locked = tryWriteLock(i);
        } while (!locked && System.nanoTime() - start < timeoutNs);

        return locked;
    }

    /**
     * Unlock the write lock
     *
     * @param i position that should be unlocked
     */
    public void unlockWrite(int i) {
        var oldValue = array.getAndSet(i, 0);

        // if lock was not write locked before, illegal state is thrown
        if(oldValue != -1) {
            throw  new IllegalReadWriteLockState("Error unlocking: Lock was not write locked before");
        }
    }

    /**
     * Convert write lock to read lock
     *
     * @param i position where lock should be converted
     */
    public void convertWriteToReadLock(int i) {
        // since write lock is exclusive, the new value should be 1
        var oldValue = array.getAndSet(i, 1);

        // if lock was not write locked before, illegal state is thrown
        if(oldValue != -1) {
            throw  new IllegalReadWriteLockState("Error converting write to read lock: Lock was not write locked before");
        }
    }

    /**
     * Check if position is write locked
     *
     * @param i position to check
     * @return boolean indicating if position is write locked
     */
    public boolean isWriteLocked(int i) {
        return array.get(i) < 0;
    }

    /**
     * Check if position is read locked
     *
     * @param i position to check
     * @return boolean indicating if position is read locked
     */
    public boolean isReadLocked(int i) {
        return array.get(i) > 0;
    }

    /**
     * A RuntimeException if lock enters an illeagal state
     */
    public class IllegalReadWriteLockState extends  RuntimeException {

        /**
         * Instantiates a new Illegal read write lock state.
         *
         * @param message the message
         */
        public IllegalReadWriteLockState(String message) {
            super(message);
        }
    }
}
