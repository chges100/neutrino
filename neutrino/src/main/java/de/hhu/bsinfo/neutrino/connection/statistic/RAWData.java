package de.hhu.bsinfo.neutrino.connection.statistic;

public class RAWData {
    public Statistic.KeyType[] keyTypes;
    public long[] keyData;
    public Statistic.Metric[] metrics;
    public long[] metricData;

    public void setKeyTypes(Statistic.KeyType ... keyTypes) {
        this.keyTypes = keyTypes.clone();
    }

    public void setKeyData(long ... data) {
        this.keyData = data.clone();
    }

    public void setMetrics(Statistic.Metric ... metrics) {
        this.metrics = metrics.clone();
    }

    public void setMetricsData(long ... data) {
        this.metricData = data.clone();
    }
}
