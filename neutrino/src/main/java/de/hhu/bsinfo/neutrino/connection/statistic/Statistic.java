package de.hhu.bsinfo.neutrino.connection.statistic;

import org.agrona.collections.Long2LongCounterMap;

public class Statistic {
    public final Long2LongCounterMap statistic;
    public final KeyType keyType;
    public final Metric metric;

    public Statistic(KeyType keyType, Metric metric) {
        this.keyType = keyType;
        this.metric = metric;

        this.statistic = new Long2LongCounterMap(0);
    }

    public enum KeyType {
        REMOTE_LID,
        QP_NUM,
        CONNECTION_ID
    }

    public enum Metric {
        SEND_MSG,
        RECEIVE_MSG,
        BYTES_SEND,
        BYTES_RECEIVED,
        RDMA_WRITE,
        RDMA_READ,
        RDMA_BYTES_WRITTEN,
        RDMA_BYTES_READ,
        SEND_QUEUE_ERRORS,
        RECEIVE_QUEUE_ERRORS
    }
}
