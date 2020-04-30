package de.hhu.bsinfo.neutrino.connection.statistic;

import de.hhu.bsinfo.neutrino.util.Poolable;
import de.hhu.bsinfo.neutrino.verbs.SendWorkRequest;
import org.agrona.collections.Long2LongCounterMap;

import java.util.concurrent.atomic.AtomicLong;

public class RequestStatistic {
    public final short remoteLocalId;
    public final AtomicLong rdmaBytesWritten = new AtomicLong(0);
    public final AtomicLong rdmaBytesRead = new AtomicLong(0);
    public final AtomicLong bytesSent = new AtomicLong(0);
    public final AtomicLong bytesReceived = new AtomicLong(0);

    public final AtomicLong rdmaWriteCount = new AtomicLong(0);
    public final AtomicLong rdmaReadCount = new AtomicLong(0);
    public final AtomicLong sendCount = new AtomicLong(0);
    public final AtomicLong receiveCount = new AtomicLong(0);
    public final AtomicLong otherOpCount = new AtomicLong(0);
    public final AtomicLong errorCount = new AtomicLong(0);

    public RequestStatistic(short remoteLocalId) {
        this.remoteLocalId = remoteLocalId;
    }
}
