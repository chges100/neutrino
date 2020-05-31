package de.hhu.bsinfo.neutrino.example.measurement;

import org.json.JSONObject;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 * Represents a measurement of throughput
 * Originally imported from de.hhu.bsinfo.observatory.benchmark.result
 *
 * @author edited by Christian Gesse
 */
public class ThroughputMeasurement extends Measurement {

    /**
     * Total duration of measurement
     */
    private double totalTime;
    /**
     * Operation throughput of measurement
     */
    private double operationThroughput;
    /**
     * Data throughput of measurement
     */
    private double dataThroughput;

    /**
     * Type of measurement
     */
    private final String measurementType = "throughput";
    /**
     * Unit for amounts of data
     */
    private final String unit = "bytes";
    /**
     * Operation mode (RDMA Read/Write)
     */
    private final String mode;

    /**
     * Instantiates a new Throughput measurement.
     *
     * @param nodes            the count of nodes
     * @param threadsPerRemote the threads per remote
     * @param localId          the local id
     * @param operationCount   the operation count
     * @param operationSize    the operation size
     * @param mode             the mode (Read/Write)
     */
    public ThroughputMeasurement(long nodes, long threadsPerRemote, long localId, long operationCount, long operationSize, String mode) {
        super(nodes, threadsPerRemote, localId, operationCount, operationSize);

        this.mode = mode;
    }

    public double getTotalTime() {
        return totalTime;
    }

    /**
     * Sets measured time.
     *
     * @param timeInNanos the time in nanos
     */
    public void setMeasuredTime(long timeInNanos) {
        this.totalTime = timeInNanos / 1000000000d;

        //calculate some statistics
        operationThroughput = (double) getOperationCount() / totalTime;
        dataThroughput = (double) getTotalData() / totalTime;
    }

    /**
     * Gets operation throughput.
     *
     * @return the operation throughput in op/s
     */
    public double getOperationThroughput() {
        return operationThroughput;
    }

    /**
     * Gets data throughput.
     *
     * @return the data throughput in bytes/s
     */
    public double getDataThroughput() {
        return dataThroughput;
    }

    /**
     * Sets total time.
     *
     * @param timeInSeconds the time in seconds
     */
    public void setTotalTime(double timeInSeconds) {
        this.totalTime = timeInSeconds;
    }

    /**
     * Sets operation throughput.
     *
     * @param operationThroughput the operation throughput in op/s
     */
    public void setOperationThroughput(double operationThroughput) {
        this.operationThroughput = operationThroughput;
    }

    /**
     * Sets data throughput.
     *
     * @param dataThroughput the data throughput in bytes/s
     */
    public void setDataThroughput(double dataThroughput) {
        this.dataThroughput = dataThroughput;
    }

    /**
     * Builds string with measurement data and information
     *
     * @return string with measurement data
     */
    @Override
    public String toString() {
        return "ThroughputMeasurement " + mode  + " {" +
            "\n\t" + ValueFormatter.formatValue("locadId", localId) +
            ",\n\t" + ValueFormatter.formatValue("nodes", nodes) +
            ",\n\t" + ValueFormatter.formatValue("threadsPerRemote", threadsPerRemote) +
            ",\n\t" + ValueFormatter.formatValue("operationCount", getOperationCount()) +
            ",\n\t" + ValueFormatter.formatValue("operationSize", getOperationSize(), "Byte") +
            ",\n\t" + ValueFormatter.formatValue("totalData", getTotalData(), "Byte") +
            ",\n\t" + ValueFormatter.formatValue("totalTime", totalTime, "s") +
            ",\n\t" + ValueFormatter.formatValue("operationThroughput", operationThroughput, "Operations/s") +
            ",\n\t" + ValueFormatter.formatValue("dataThroughput", dataThroughput, "Byte/s") +
            "\n}";
    }

    /**
     * Saves measurement data into JSON File
     *
     * @throws IOException if file handling goes wrong
     */
    @Override
    public void toJSON() throws IOException {
        JSONObject json = new JSONObject();

        json.put("measurementType", measurementType + mode);
        json.put("nodes", nodes);
        json.put("threadsPerRemote", threadsPerRemote);
        json.put("timestamp", timestampMs);
        json.put("localId", localId);

        JSONObject jsonMeasurement = new JSONObject();
        jsonMeasurement.put("totalTimeInSeconds", totalTime);
        jsonMeasurement.put("operationSize", operationSize);
        jsonMeasurement.put("operationCount", operationCount);
        jsonMeasurement.put("operationThroughput", operationThroughput);
        jsonMeasurement.put("dataThroughput", dataThroughput);
        jsonMeasurement.put("dataUnit", unit);
        jsonMeasurement.put("totalData", totalData);

        json.put("measurement", jsonMeasurement);

        var currentDir = System.getProperty("user.dir");

        var file = new File(currentDir + "/measurements/" + measurementType + "/measurement_localId" + localId + "_n" + nodes + "_t" + threadsPerRemote + "_" + System.currentTimeMillis() + ".json");
        file.getParentFile().mkdirs();

        var fileWriter = new FileWriter(file);
        fileWriter.write(json.toString());
        fileWriter.close();
    }
}
