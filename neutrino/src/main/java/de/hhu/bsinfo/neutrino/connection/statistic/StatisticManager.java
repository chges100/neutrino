package de.hhu.bsinfo.neutrino.connection.statistic;

import org.jctools.maps.NonBlockingHashMapLong;

import java.util.concurrent.atomic.AtomicLong;

public class StatisticManager {
    public final AtomicLong executeIdProvider = new AtomicLong(1);

    private final NonBlockingHashMapLong<RequestStatistic> requestStatisticMap = new NonBlockingHashMapLong<>();
    private final NonBlockingHashMapLong<LatencyStatistic> connectLatencyStatisticMap = new NonBlockingHashMapLong<>();
    private final NonBlockingHashMapLong<LatencyStatistic> executeLatencyStatisticMap = new NonBlockingHashMapLong<>();

    public void registerRemote(short remoteLocalId) {
        requestStatisticMap.putIfAbsent(remoteLocalId, new RequestStatistic(remoteLocalId));
    }

    public void startConnectLatencyStatistic(long id, long startTime) {

        var statistic = new LatencyStatistic();
        statistic.id = id;
        statistic.startTime = startTime;

        connectLatencyStatisticMap.putIfAbsent(id, statistic);
    }

    public void endConnectLatencyStatistic(long id, long endTime) {
       connectLatencyStatisticMap.get(id).endTime = endTime;
    }

    public void startExecuteLatencyStatistic(long id, long startTime) {

        var statistic = new LatencyStatistic();
        statistic.id = id;
        statistic.startTime = startTime;

        executeLatencyStatisticMap.putIfAbsent(id, statistic);
    }

    public void endExecuteLatencyStatistic(long id, long endTime) {
        executeLatencyStatisticMap.get(id).endTime = endTime;
    }

    public void putSendEvent(short remoteLocalId, long bytesSend) {
        var statistic = requestStatisticMap.get(remoteLocalId);

        if(statistic != null) {
            statistic.bytesSent.addAndGet(bytesSend);
            statistic.sendCount.incrementAndGet();
        }
    }

    public void putReceiveEvent(short remoteLocalId, long bytesReceived) {
        var statistic = requestStatisticMap.get(remoteLocalId);

        if(statistic != null) {
            statistic.bytesReceived.addAndGet(bytesReceived);
            statistic.receiveCount.incrementAndGet();
        }
    }

    public void putRDMAWriteEvent(short remoteLocalId, long bytesWritten) {
        var statistic = requestStatisticMap.get(remoteLocalId);

        if(statistic != null) {
            statistic.rdmaBytesWritten.addAndGet(bytesWritten);
            statistic.rdmaWriteCount.incrementAndGet();
        }
    }

    public void putRDMAReadEvent(short remoteLocalId, long bytesRead) {
        var statistic = requestStatisticMap.get(remoteLocalId);

        if(statistic != null) {
            statistic.rdmaBytesRead.addAndGet(bytesRead);
            statistic.rdmaReadCount.incrementAndGet();
        }
    }

    public void putOtherOpEvent(short remoteLocalId) {
        var statistic = requestStatisticMap.get(remoteLocalId);

        if(statistic != null) {
            statistic.otherOpCount.incrementAndGet();
        }
    }

    public void putErrorEvent(short remoteLocalId) {
        var statistic = requestStatisticMap.get(remoteLocalId);

        if(statistic != null) {
            statistic.errorCount.incrementAndGet();
        }
    }

    public long getTotalRDMABytesWritten() {
        long ret = 0;

        for(var statistic : requestStatisticMap.values()) {
            ret += statistic.rdmaBytesWritten.get();
        }

        return ret;
    }

    public long getTotalRDMABytesRead() {
        long ret = 0;

        for(var statistic : requestStatisticMap.values()) {
            ret += statistic.rdmaBytesRead.get();
        }

        return ret;
    }

    public long getTotalBytesSent() {
        long ret = 0;

        for(var statistic : requestStatisticMap.values()) {
            ret += statistic.bytesSent.get();
        }

        return ret;
    }

    public long getTotalBytesReceived() {
        long ret = 0;

        for(var statistic : requestStatisticMap.values()) {
            ret += statistic.bytesReceived.get();
        }

        return ret;
    }

    public long getTotalSendCount() {
        long ret = 0;

        for(var statistic : requestStatisticMap.values()) {
            ret += statistic.sendCount.get();
        }

        return ret;
    }

    public long getTotalReceiveCount() {
        long ret = 0;

        for(var statistic : requestStatisticMap.values()) {
            ret += statistic.receiveCount.get();
        }

        return ret;
    }

    public long getTotalRDMAWriteCount() {
        long ret = 0;

        for(var statistic : requestStatisticMap.values()) {
            ret += statistic.rdmaWriteCount.get();
        }

        return ret;
    }

    public long getTotalRDMAReadCount() {
        long ret = 0;

        for(var statistic : requestStatisticMap.values()) {
            ret += statistic.rdmaReadCount.get();
        }

        return ret;
    }

    public long getTotalOtherOpCount() {
        long ret = 0;

        for(var statistic : requestStatisticMap.values()) {
            ret += statistic.otherOpCount.get();
        }

        return ret;
    }

    public long getTotalErrorCount() {
        long ret = 0;

        for(var statistic : requestStatisticMap.values()) {
            ret += statistic.errorCount.get();
        }

        return ret;
    }

    public long[] getConnectLatencies() {
        var data = connectLatencyStatisticMap.values();
        long[] latencies = new long[data.size()];

        int idx = 0;

        for(var statistic : data) {
            latencies[idx] = statistic.endTime - statistic.startTime;

            idx++;
        }

        return latencies;
    }

    public long[] getExecuteLatencies() {
        var data = executeLatencyStatisticMap.values();
        long[] latencies = new long[data.size()];

        int idx = 0;

        for(var statistic : data) {
            latencies[idx] = statistic.endTime - statistic.startTime;

            idx++;
        }

        return latencies;
    }

}
