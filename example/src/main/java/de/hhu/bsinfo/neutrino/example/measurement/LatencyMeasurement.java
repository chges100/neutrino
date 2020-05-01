package de.hhu.bsinfo.neutrino.example.measurement;

import java.util.HashMap;
import java.util.Map;

/**
 * Originally imported from de.hhu.bsinfo.observatory.benchmark.result
 **/
public class LatencyMeasurement extends Measurement {

    private final HashMap<String, LatencyStatistics> latencyMap = new HashMap<>();

    public LatencyMeasurement(long operationCount, long operationSize) {
        super(operationCount, operationSize);
    }

    public void addLatencyMeasurement(String name, long ... latencies) {
        var latencyStatistics = new LatencyStatistics();

        latencyStatistics.setLatencies(latencies);
        latencyStatistics.sortAscending();

        latencyMap.put(name, latencyStatistics);
    }

    public double getAverageLatency(String name) {
        return latencyMap.get(name).getAvgNs() / 1000000000;
    }

    public double getMinimumLatency(String name) {
        return latencyMap.get(name).getMinNs() / 1000000000;
    }

    public double getMaximumLatency(String name) {
        return latencyMap.get(name).getMaxNs() / 1000000000;
    }

    public double getPercentileLatency(String name, float percentile) {
        return latencyMap.get(name).getPercentilesNs(percentile) / 1000000000;
    }

    @Override
    public double getTotalTime() {
        return 0;
    }

    @Override
    public String toString() {
        var ret = "LatencyMeasurement {";

        ret += "\n\t" + ValueFormatter.formatValue("operationCount", getOperationCount());
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
}