package de.hhu.bsinfo.neutrino.connection.util;

public class Pair<K,V> {
    private final K key;
    private final V value;

    public Pair(K key, V value) {
        this.key = key;
        this.value = value;
    }

    public V getValue() {
        return value;
    }

    public K getKey() {
        return key;
    }

    public static class IndexStampPair extends Pair<Integer, Long> {
        public IndexStampPair(int idx, long stamp) {
            super(idx, stamp);
        }

        public int getIndex() {
            return super.getKey();
        }

        public long getStamp() {
            return super.getValue();
        }
    }
}
