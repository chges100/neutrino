package de.hhu.bsinfo.neutrino.connection.util;

import org.agrona.concurrent.ManyToManyConcurrentArrayQueue;

import java.util.ArrayList;

public class ConcurrentRingBuffer<T> {

    private final ManyToManyConcurrentArrayQueue<T> buffer;

    public ConcurrentRingBuffer(final int size) {
        buffer = new ManyToManyConcurrentArrayQueue<>(size);
    }

    public int size() {
        return buffer.size();
    }

    public ArrayList<T> clear() {
        var ret = new ArrayList<T>();
        buffer.removeAll(ret);

        return ret;
    }

    public boolean isEmpty() {
        return buffer.isEmpty();
    }

    public boolean isFull() {
        return buffer.remainingCapacity() == 0;
    }

    public void push(final T object) {
        if(!isFull()) {
            buffer.offer(object);
        }
    }

    public T pop() {
        return buffer.poll();
    }
}
