package de.hhu.bsinfo.neutrino.example.measurement;

import java.io.IOException;

/**
 * Originally imported from de.hhu.bsinfo.observatory.benchmark.result
 **/
public abstract class Measurement implements Cloneable {

    protected final long operationCount;
    protected final long operationSize;

    protected final long nodes;
    protected final long threadsPerNode;
    protected final long timestampMs;
    protected final long localId;

    protected long totalData;

    Measurement(long nodes, long threadsPerNode, long localId, long operationCount, long operationSize) {
        this.nodes = nodes;
        this.threadsPerNode = threadsPerNode;
        this.localId = localId;
        this.operationCount = operationCount;
        this.operationSize = operationSize;
        totalData = (long) operationCount * (long) operationSize;

        timestampMs = System.currentTimeMillis();
    }

    public long getOperationCount() {
        return operationCount;
    }

    public long getOperationSize() {
        return operationSize;
    }

    public long getTotalData() {
        return totalData;
    }

    public abstract double getTotalTime();

    public void setTotalData(long totalData) {
        this.totalData = totalData;
    }

    @Override
    public String toString() {
        return "Measurement {" +
            "\n\t" + ValueFormatter.formatValue("operationCount", operationCount) +
            ",\n\t" + ValueFormatter.formatValue("operationSize", operationSize, "Byte") +
            ",\n\t" + ValueFormatter.formatValue("totalData", totalData, "Byte") +
            "\n}";
    }

    public abstract void toJSON() throws IOException;
}
