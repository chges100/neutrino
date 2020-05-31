package de.hhu.bsinfo.neutrino.example.measurement;

import java.io.IOException;

/**
 * Basic class for measurements.
 * Originally imported from de.hhu.bsinfo.observatory.benchmark.result
 *
 * @author edited by Christian Gesse
 */
public abstract class Measurement implements Cloneable {

    /**
     * The count of operations
     */
    protected final long operationCount;
    /**
     * The size of each operation
     */
    protected final long operationSize;

    /**
     * The number of nodes
     */
    protected final long nodes;
    /**
     * The parallel worker threads per remote
     */
    protected final long threadsPerRemote;
    /**
     * The timestamp of this measurement
     */
    protected final long timestampMs;
    /**
     * The Local id of this node
     */
    protected final long localId;

    /**
     * The total amount of data transferred during operations of this measurement in bytes
     */
    protected long totalData;

    /**
     * Instantiates a new Measurement.
     *
     * @param nodes            the number of nodes
     * @param threadsPerRemote the threads per remote
     * @param localId          the local id of this node
     * @param operationCount   the operation count
     * @param operationSize    the operation size
     */
    Measurement(long nodes, long threadsPerRemote, long localId, long operationCount, long operationSize) {
        this.nodes = nodes;
        this.threadsPerRemote = threadsPerRemote;
        this.localId = localId;
        this.operationCount = operationCount;
        this.operationSize = operationSize;
        totalData = (long) operationCount * (long) operationSize;

        timestampMs = System.currentTimeMillis();
    }

    /**
     * Gets operation count.
     *
     * @return the operation count
     */
    public long getOperationCount() {
        return operationCount;
    }

    /**
     * Gets operation size.
     *
     * @return the operation size in bytes
     */
    public long getOperationSize() {
        return operationSize;
    }

    /**
     * Gets thie total data transfered by operations on this node
     *
     * @return the total data in bytes
     */
    public long getTotalData() {
        return totalData;
    }

    /**
     * Gets the elapsed time during this measurement
     *
     * @return the total time
     */
    public abstract double getTotalTime();

    /**
     * Sets the amount total data.
     *
     * @param totalData the total data in bytes
     */
    public void setTotalData(long totalData) {
        this.totalData = totalData;
    }

    /**
     * Generates string with data of measurement
     *
     * @return string with data of measurement
     */
    @Override
    public String toString() {
        return "Measurement {" +
            "\n\t" + ValueFormatter.formatValue("operationCount", operationCount) +
            ",\n\t" + ValueFormatter.formatValue("operationSize", operationSize, "Byte") +
            ",\n\t" + ValueFormatter.formatValue("totalData", totalData, "Byte") +
            "\n}";
    }

    /**
     * Generates and saves JSON Object with all data of this measurement
     *
     * @throws IOException thrown if file handling goes wrong
     */
    public abstract void toJSON() throws IOException;
}
