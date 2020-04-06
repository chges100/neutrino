package de.hhu.bsinfo.neutrino.connection.statistic;

import de.hhu.bsinfo.neutrino.connection.ReliableConnection;
import org.agrona.collections.BiInt2ObjectMap;
import org.agrona.concurrent.ManyToOneConcurrentArrayQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class StatisticManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(StatisticManager.class);

    private final int QUEUE_SIZE = 4000;
    private final int BATCH_SIZE = 200;

    private final BiInt2ObjectMap<Statistic> statistics;
    private final ManyToOneConcurrentArrayQueue<RAWData> rawData;
    private final Handler handlerThread;

    public StatisticManager() {
        statistics = new BiInt2ObjectMap<>();
        rawData = new ManyToOneConcurrentArrayQueue<>(QUEUE_SIZE);

        handlerThread = new Handler();
        handlerThread.start();
    }

    public boolean registerStatistic(Statistic.KeyType keyType, Statistic.Metric metric) {
        if(statistics.get(keyType.ordinal(), metric.ordinal()) == null) {
            statistics.put(keyType.ordinal(), metric.ordinal(), new Statistic(keyType, metric));

            return true;
        } else {
            return false;
        }
    }

    public Statistic getStatistic(Statistic.KeyType keyType, Statistic.Metric metric) {
        return statistics.get(keyType.ordinal(), metric.ordinal());
    }

    public void pushRAWData(RAWData data) {
        rawData.offer(data);
    }

    public void shutDown() {
        handlerThread.shutDown();
    }

    public void printResults() {
        final String[] out = {"\n\n*** Print out statistic results: ***\n\n"};

        statistics.forEach(statistic -> {
            out[0] += "Statistic:\t" + statistic.keyType + "\t" + statistic.metric + "\n";
            statistic.statistic.forEach((key, value) -> {
                out[0] += "\t\t" + key + "\t\t" + value + "\n";
            });
            out[0] += "\n";
        });

        LOGGER.info(out[0]);
    }

    public class Handler extends Thread {
        private boolean isRunning = true;

        @Override
        public void run() {

            while (isRunning) {
                rawData.drain(data -> {
                    for(int i = 0; i < data.keyTypes.length; i++) {
                        for(int j = 0; j < data.metrics.length; j++) {
                            try {
                                var statistic = statistics.get(data.keyTypes[i].ordinal(), data.metrics[j].ordinal());
                                statistic.statistic.addAndGet(data.keyData[i], data.metricData[j]);
                            } catch (NullPointerException e) {
                                LOGGER.trace("No statistic for this key/value avilable");
                            }

                        }
                    }
                }, BATCH_SIZE);
            }
        }

        public void shutDown() {
            isRunning = false;
        }
    }
}
