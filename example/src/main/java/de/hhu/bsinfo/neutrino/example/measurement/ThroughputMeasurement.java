package de.hhu.bsinfo.neutrino.example.measurement;

import org.json.JSONObject;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 * Originally imported from de.hhu.bsinfo.observatory.benchmark.result
 **/
public class ThroughputMeasurement extends Measurement {

    private double totalTime;
    private double operationThroughput;
    private double dataThroughput;

    private final String measurementType = "throughput";
    private final String unit = "bytes";

    public ThroughputMeasurement(long nodes, long threadsPerRemote, long localId, long operationCount, long operationSize) {
        super(nodes, threadsPerRemote, localId, operationCount, operationSize);
    }

    public double getTotalTime() {
        return totalTime;
    }

    public void setMeasuredTime(long timeInNanos) {
        this.totalTime = timeInNanos / 1000000000d;

        operationThroughput = (double) getOperationCount() / totalTime;
        dataThroughput = (double) getTotalData() / totalTime;
    }

    public double getOperationThroughput() {
        return operationThroughput;
    }

    public double getDataThroughput() {
        return dataThroughput;
    }

    public void setTotalTime(double timeInSeconds) {
        this.totalTime = timeInSeconds;
    }

    public void setOperationThroughput(double operationThroughput) {
        this.operationThroughput = operationThroughput;
    }

    public void setDataThroughput(double dataThroughput) {
        this.dataThroughput = dataThroughput;
    }

    @Override
    public String toString() {
        return "ThroughputMeasurement {" +
            "\n\t" + ValueFormatter.formatValue("operationCount", getOperationCount()) +
            ",\n\t" + ValueFormatter.formatValue("operationSize", getOperationSize(), "Byte") +
            ",\n\t" + ValueFormatter.formatValue("totalData", getTotalData(), "Byte") +
            ",\n\t" + ValueFormatter.formatValue("totalTime", totalTime, "s") +
            ",\n\t" + ValueFormatter.formatValue("operationThroughput", operationThroughput, "Operations/s") +
            ",\n\t" + ValueFormatter.formatValue("dataThroughput", dataThroughput, "Byte/s") +
            "\n}";
    }

    @Override
    public void toJSON() throws IOException {
        JSONObject json = new JSONObject();

        json.put("measurementType", measurementType);
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
