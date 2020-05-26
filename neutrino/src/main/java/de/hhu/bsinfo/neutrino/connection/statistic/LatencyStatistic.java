package de.hhu.bsinfo.neutrino.connection.statistic;

/**
 * Statistic for a latency (or duration of an operation)
 *
 * @author Christian Gesse
 */
public class LatencyStatistic {

    /**
     * Id of this statistic
     */
    public long id;
    /**
     * Start time of the operation or latency
     */
    public long startTime;
    /**
     * End time of the operation or latency
     */
    public long endTime;
}
