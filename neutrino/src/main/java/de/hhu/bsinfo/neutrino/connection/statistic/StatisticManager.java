package de.hhu.bsinfo.neutrino.connection.statistic;

import org.jctools.maps.NonBlockingHashMapLong;

public class StatisticManager {
    private NonBlockingHashMapLong<Statistic> statisticMap;

    public StatisticManager() {
        statisticMap = new NonBlockingHashMapLong<>();
    }

    public void registerRemote(short remoteLocalId) {
        statisticMap.putIfAbsent(remoteLocalId, new Statistic(remoteLocalId));
    }

    public void putSendEvent(short remoteLocalId, long bytesSend) {
        var statistic = statisticMap.get(remoteLocalId);

        if(statistic != null) {
            statistic.bytesSent.addAndGet(bytesSend);
            statistic.sendCount.incrementAndGet();
        }
    }

    public void putReceiveEvent(short remoteLocalId, long bytesReceived) {
        var statistic = statisticMap.get(remoteLocalId);

        if(statistic != null) {
            statistic.bytesReceived.addAndGet(bytesReceived);
            statistic.receiveCount.incrementAndGet();
        }
    }

    public void putRDMAWriteEvent(short remoteLocalId, long bytesWritten) {
        var statistic = statisticMap.get(remoteLocalId);

        if(statistic != null) {
            statistic.rdmaBytesWritten.addAndGet(bytesWritten);
            statistic.rdmaWriteCount.incrementAndGet();
        }
    }

    public void putRDMAReadEvent(short remoteLocalId, long bytesRead) {
        var statistic = statisticMap.get(remoteLocalId);

        if(statistic != null) {
            statistic.rdmaBytesRead.addAndGet(bytesRead);
            statistic.rdmaReadCount.incrementAndGet();
        }
    }

    public void putOtherOpEvent(short remoteLocalId) {
        var statistic = statisticMap.get(remoteLocalId);

        if(statistic != null) {
            statistic.otherOpCount.incrementAndGet();
        }
    }

    public void putErrorEvent(short remoteLocalId) {
        var statistic = statisticMap.get(remoteLocalId);

        if(statistic != null) {
            statistic.errorCount.incrementAndGet();
        }
    }

    public long getTotalRDMABytesWritten() {
        long ret = 0;

        for(var statistic : statisticMap.values()) {
            ret += statistic.rdmaBytesWritten.get();
        }

        return ret;
    }

    public long getTotalRDMABytesRead() {
        long ret = 0;

        for(var statistic : statisticMap.values()) {
            ret += statistic.rdmaBytesRead.get();
        }

        return ret;
    }

    public long getTotalBytesSent() {
        long ret = 0;

        for(var statistic : statisticMap.values()) {
            ret += statistic.bytesSent.get();
        }

        return ret;
    }

    public long getTotalBytesReceived() {
        long ret = 0;

        for(var statistic : statisticMap.values()) {
            ret += statistic.bytesReceived.get();
        }

        return ret;
    }

    public long getTotalSendCount() {
        long ret = 0;

        for(var statistic : statisticMap.values()) {
            ret += statistic.sendCount.get();
        }

        return ret;
    }

    public long getTotalReceiveCount() {
        long ret = 0;

        for(var statistic : statisticMap.values()) {
            ret += statistic.receiveCount.get();
        }

        return ret;
    }

    public long getTotalRDMAWriteCount() {
        long ret = 0;

        for(var statistic : statisticMap.values()) {
            ret += statistic.rdmaWriteCount.get();
        }

        return ret;
    }

    public long getTotalRDMAReadCount() {
        long ret = 0;

        for(var statistic : statisticMap.values()) {
            ret += statistic.rdmaReadCount.get();
        }

        return ret;
    }

    public long getTotalOtherOpCount() {
        long ret = 0;

        for(var statistic : statisticMap.values()) {
            ret += statistic.otherOpCount.get();
        }

        return ret;
    }

    public long getTotalErrorCount() {
        long ret = 0;

        for(var statistic : statisticMap.values()) {
            ret += statistic.errorCount.get();
        }

        return ret;
    }

}
