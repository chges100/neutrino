package de.hhu.bsinfo.neutrino.connection.dynamic;

import java.util.concurrent.atomic.AtomicIntegerArray;


/**
 * Simple table that indicates if a reliable connection was used in the last timer period
 *
 * @author Christian Gesse
 */
public class RCUsageTable {
    /**
     * Constant indicating used status
     */
    public static final int RC_USED = 1;
    /**
     * Constant indicating unused status
     */
    public static final int RC_UNSUSED = 0;

    /**
     * The table itself. Consists of an Atomic Array with low memory footprint.
     */
    private final AtomicIntegerArray rcUsageTable;

    /**
     * Maximum number of entries
     */
    private final int size;

    /**
     * Instantiates a new Rc usage table.
     *
     * @param size number of entries
     */
    public RCUsageTable(int size) {
        rcUsageTable = new AtomicIntegerArray(size);
        this.size = size;
    }

    /**
     * Sets a connection entry to used
     *
     * @param i local id of the connection's remote
     */
    public void setUsed(int i) {
        rcUsageTable.set(i, RC_USED);
    }

    /**
     * Sets a connection entry to unused
     *
     * @param i local id of the connection's remote
     */
    public void setUnused(int i) {
        rcUsageTable.set(i, RC_UNSUSED);
    }

    /**
     * Gets status of a connection entry
     *
     * @param i local id of the connection's remote
     * @return the status
     */
    public int getStatus(int i) {
        return rcUsageTable.get(i);
    }

    /**
     * Gets status of connection entry and resets it
     *
     * @param i local id of the connection's remote
     * @return the status and reset
     */
    public int getStatusAndReset(int i) {
        return rcUsageTable.getAndSet(i, RC_UNSUSED);
    }

    /**
     * Gets the number of possible table entries
     *
     * @return the size
     */
    public int getSize() {
        return size;
    }
}
