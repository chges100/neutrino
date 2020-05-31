package de.hhu.bsinfo.neutrino.example.measurement;

import org.json.JSONArray;
import org.json.JSONObject;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;

/**
 * Represents measurement of latencies.
 * Originally imported from de.hhu.bsinfo.observatory.benchmark.result
 *
 * @author edited by Christian Gesse
 */
public class LatencyMeasurement extends Measurement {

    /**
     * Hashmap conatining all latency statistics by name
     * Names are used to differentiate between different latency types
     */
    private final HashMap<String, LatencyStatistics> latencyMap = new HashMap<>();

    /**
     * Time unit for measurements
     */
    private final String unit = "ns";
    /**
     * Type of measurement
     */
    private final String measurementType = "latency";

    /**
     * Instantiates a new Latency measurement.
     *
     * @param nodes            the number nodes
     * @param threadsPerRemote the threads per remote
     * @param localId          the local id
     * @param operationCount   the operation count
     * @param operationSize    the operation size in bytes
     */
    public LatencyMeasurement(long nodes, long threadsPerRemote, long localId, long operationCount, long operationSize) {
        super(nodes, threadsPerRemote, localId, operationCount, operationSize);
    }

    /**
     * Add one latency measurement to latency statistic
     *
     * @param name      the type of the latency
     * @param latencies the latencies as long array
     */
    public void addLatencyMeasurement(String name, long ... latencies) {
        var latencyStatistics = new LatencyStatistics();

        latencyStatistics.setLatencies(latencies);
        latencyStatistics.sortAscending();

        latencyMap.put(name, latencyStatistics);
    }

    /**
     * Gets average latency for one latency type
     *
     * @param name the name of the latency
     * @return the average latency
     */
    public double getAverageLatency(String name) {
        return latencyMap.get(name).getAvgNs() / 1000000000;
    }

    /**
     * Gets minimum latency for one latency type
     *
     * @param name the name of the latency
     * @return the minimum latency
     */
    public double getMinimumLatency(String name) {
        return latencyMap.get(name).getMinNs() / 1000000000;
    }

    /**
     * Gets maximum latency for one latency type
     *
     * @param name the name of the latency
     * @return the maximum latency
     */
    public double getMaximumLatency(String name) {
        return latencyMap.get(name).getMaxNs() / 1000000000;
    }

    /**
     * Gets percentile latency for one latency type
     *
     * @param name       the name of the latency
     * @param percentile the percentile
     * @return the percentile latency
     */
    public double getPercentileLatency(String name, float percentile) {
        return latencyMap.get(name).getPercentilesNs(percentile) / 1000000000;
    }

    /**
     * Overwritten since there is no use for this measurement type
     *
     * @return constant 0
     */
    @Override
    public double getTotalTime() {
        return 0;
    }

    /**
     * Builds string with measurement data and information
     *
     * @return string with measurement data
     */
    @Override
    public String toString() {
        var ret = "LatencyMeasurement {";
        ret += "\n\t" + ValueFormatter.formatValue("locadId", localId);
        ret += ",\n\t" + ValueFormatter.formatValue("nodes", nodes);
        ret += ",\n\t" + ValueFormatter.formatValue("threadsPerRemote", threadsPerRemote) ;
        ret += ",\n\t" + ValueFormatter.formatValue("operationCount", getOperationCount());
        ret += ",\n\t" + ValueFormatter.formatValue("operationSize", getOperationSize(), "Byte");
        ret += ",\n\t" + ValueFormatter.formatValue("totalData", getTotalData(), "Byte");

        for(var kv : latencyMap.entrySet()) {
            var name = kv.getKey();
            var statistic = kv.getValue();

            ret += "\n\t" + name +" latencies:";

            ret += "\n\t\t" + ValueFormatter.formatValue("latenyCount", statistic.getLatencyCount());
            ret += ",\n\t\t" + ValueFormatter.formatValue("averageLatency", getAverageLatency(name), "s");
            ret += ",\n\t\t" + ValueFormatter.formatValue("minimumLatency", getMinimumLatency(name), "s");
            ret += ",\n\t\t" + ValueFormatter.formatValue("maximumLatency", getMaximumLatency(name), "s");
            ret += ",\n\t\t" + ValueFormatter.formatValue("50% Latency", getPercentileLatency(name, 0.5f), "s");
            ret += ",\n\t\t" + ValueFormatter.formatValue("90% Latency", getPercentileLatency(name, 0.9f), "s");
            ret += ",\n\t\t" + ValueFormatter.formatValue("95% Latency", getPercentileLatency(name, 0.95f), "s");
            ret += ",\n\t\t" + ValueFormatter.formatValue("99% Latency", getPercentileLatency(name, 0.99f), "s");
            ret += ",\n\t\t" + ValueFormatter.formatValue("99.9% Latency", getPercentileLatency(name, 0.999f), "s");
            ret += ",\n\t\t" + ValueFormatter.formatValue("99.99% Latency", getPercentileLatency(name, 0.9999f), "s");
        }

        ret += "\n}";

        return ret;
    }

    /**
     * Saves measurement data into JSON File
     *
     * @throws IOException if file handling goes wrong
     */
    @Override
    public void toJSON() throws IOException {
        for(var entry : latencyMap.entrySet()) {
            var name = entry.getKey();
            var latency = entry.getValue();

            JSONObject json = new JSONObject();

            json.put("measurementType", measurementType);
            json.put("nodes", nodes);
            json.put("threadsPerRemote", threadsPerRemote);
            json.put("timestamp", timestampMs);
            json.put("localId", localId);

            JSONObject jsonMeasurement = new JSONObject();
            jsonMeasurement.put("operationSize", operationSize);
            jsonMeasurement.put("operationCount", operationCount);
            jsonMeasurement.put("timeUnit", unit);
            jsonMeasurement.put("latencies", new JSONArray(latency.getLatencies()));
            jsonMeasurement.put("avgLatency", latency.getAvgNs());
            jsonMeasurement.put("maxLatency", latency.getMaxNs());
            jsonMeasurement.put("minLatency", latency.getMinNs());
            jsonMeasurement.put("numLatencies", latency.getLatencyCount());
            jsonMeasurement.put("totalData", totalData);
            jsonMeasurement.put("latencyType", name);

            json.put("measurement", jsonMeasurement);

            var currentDir = System.getProperty("user.dir");

            var file = new File(currentDir + "/measurements/" + measurementType + "/" + name +"/measurement_localId" + localId + "_n" + nodes + "_t" + threadsPerRemote + "_" + System.currentTimeMillis() + ".json");
            file.getParentFile().mkdirs();

            var fileWriter = new FileWriter(file);
            fileWriter.write(json.toString());
            fileWriter.close();
        }
    }
}