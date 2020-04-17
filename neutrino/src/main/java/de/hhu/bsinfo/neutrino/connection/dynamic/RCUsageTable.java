package de.hhu.bsinfo.neutrino.connection.dynamic;

import java.util.concurrent.atomic.AtomicIntegerArray;

public class RCUsageTable {
    public static final int RC_USED = 1;
    public static final int RC_UNSUSED = 0;

    private final AtomicIntegerArray rcUsageTable;
    private final int size;

    public RCUsageTable(int size) {
        rcUsageTable = new AtomicIntegerArray(size);
        this.size = size;
    }

    public void setUsed(int i) {
        rcUsageTable.set(i, RC_USED);
    }

    public void setUnused(int i) {
        rcUsageTable.set(i, RC_UNSUSED);
    }

    public int getStatus(int i) {
        return rcUsageTable.get(i);
    }

    public int getStatusAndReset(int i) {
        return rcUsageTable.getAndSet(i, RC_UNSUSED);
    }

    public int getSize() {
        return size;
    }
}
