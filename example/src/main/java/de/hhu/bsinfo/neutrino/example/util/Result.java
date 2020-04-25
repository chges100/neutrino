package de.hhu.bsinfo.neutrino.example.util;

public class Result {

    private static final char[] metricTable = {
            0,
            'K',
            'M',
            'G',
            'T',
            'P',
            'E'
    };

    private final long operationCount;
    private final long operationSize;
    private final double totalTime;

    private final long totalData;
    private final double operationThroughput;
    private final double dataThroughput;

    public Result(long operationCount, long operationSize, long timeNanos) {
        this.operationCount = operationCount;
        this.operationSize = operationSize;
        this.totalTime = timeNanos / 1000000000d;

        totalData = operationCount * operationSize;
        operationThroughput = (double) operationCount / totalTime;
        dataThroughput = (double) totalData / totalTime;
    }

    public Result(long operationCount, long totalData, long timeNanos, int dummy) {
        this.operationCount = operationCount;
        this.totalData = totalData;
        this.totalTime = timeNanos / 1000000000d;

        this.operationSize = totalData / operationCount;
        operationThroughput = (double) operationCount / totalTime;
        dataThroughput = (double) totalData / totalTime;
    }

    private String getFormattedValue(String name, double value, String unit, int base) {
        double formattedValue = value;

        int counter = 0;
        while (formattedValue > base && counter < metricTable.length) {
            formattedValue = formattedValue / base;
            counter++;
        }

        if(value == (long) value) {
            return String.format("%-20s %.3f %c%s (%d)", name + ":", formattedValue, metricTable[counter], unit, (long) value);
        }

        return String.format("%-20s %.3f %c%s (%f)", name + ":", formattedValue, metricTable[counter], unit, value);
    }

    private String getFormattedValue(String name, double value, int base) {
        return getFormattedValue(name, value, "Units", base);
    }

    @Override
    public String toString() {
        return "Result {\n" +
                "\t" + getFormattedValue("operationCount", operationCount, 1000) +
                ",\n\t" + getFormattedValue("operationSize", operationSize, "Byte", 1024) +
                ",\n\t" + getFormattedValue("totalTime", totalTime, "s", 1000) +
                ",\n\t" + getFormattedValue("totalData", totalData, "Byte", 1024) +
                ",\n\t" + getFormattedValue("operationThroughput", operationThroughput, "Operations/s", 1000) +
                ",\n\t" + getFormattedValue("dataThroughput", dataThroughput, "Byte/s", 1024) +
                "\n}";
    }
}
