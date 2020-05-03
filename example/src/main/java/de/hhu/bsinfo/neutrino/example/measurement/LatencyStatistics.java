package de.hhu.bsinfo.neutrino.example.measurement;

import java.util.Arrays;

/*
 * Originally imported from de.hhu.bsinfo.observatory.benchmark.result
 */

/**
 * Statistics class to measure latency.
 *
 * @author Stefan Nothaas, HHU
 * @date 2018
 */
class LatencyStatistics {
    private long[] times;

    void setLatencies(long... latencies) {
        times = latencies.clone();
    }
    
    /**
     * Sort all currently available measurements in ascending order.
     */
    void sortAscending() {
        Arrays.sort(times);
    }

    /**
     * Get the lowest time in ns.
     *
     * @return Time in ns.
     */
    double getMinNs() {
        return times[0];
    }

    /**
     * Get the highest time in ns.
     *
     * @return Time in ns.
     */
    double getMaxNs() {
        sortAscending();

        return times[times.length - 1];
    }

    /**
     * Get the total time in ns.
     *
     * @return Time in ns.
     */
    double getTotalNs() {
        double tmp = 0;

        for (int i = 0; i < times.length; i++) {
            tmp += times[i];
        }

        return tmp;
    }

    long getLatencyCount() {
        return times.length;
    }

    long[] getLatencies() {
        return times.clone();
    }

    /**
     * Get the average time in ns.
     *
     * @return Time in ns.
     */
    double getAvgNs() {
        return getTotalNs() / times.length;
    }

    /**
     * Get the Xth percentiles value (Stats object must be sorted).
     *
     * @return Value in ns.
     */
    double getPercentilesNs(float perc) {
        if (perc < 0.0 || perc > 1.0) {
            throw new IllegalArgumentException("Percentage must be between 0 and 1");
        }

        return times[(int) Math.ceil(perc * times.length) - 1];
    }
}