package de.hhu.bsinfo.neutrino.connection.statistic;

import org.jctools.maps.NonBlockingHashMapLong;

public class StatisticManager {
    private NonBlockingHashMapLong<Statistic> statisticMap;

    public StatisticManager() {
        statisticMap = new NonBlockingHashMapLong<>();
    }

    public void registerRemote(short remoteLocalId) {
        statisticMap.put(remoteLocalId, new Statistic(remoteLocalId));
    }

    public void putSendEvent(short remoteLocalId, long bytesSend) {
        var statistic = statisticMap.get(remoteLocalId);

        if(statistic != null) {
            statistic.bytesSent.addAndGet(bytesSend);
            statistic.sendCound.incrementAndGet();
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

    public void putErrorEvent(short remoteLocalId) {
        var statistic = statisticMap.get(remoteLocalId);

        if(statistic != null) {
            statistic.errorCount.incrementAndGet();
        }
    }


}
