package de.hhu.bsinfo.neutrino.api.message.impl.manager;


import de.hhu.bsinfo.neutrino.api.message.impl.processor.CompletionHandler;
import de.hhu.bsinfo.neutrino.api.message.impl.processor.QueueProcessor;
import de.hhu.bsinfo.neutrino.verbs.CompletionChannel;
import de.hhu.bsinfo.neutrino.verbs.CompletionQueue;
import de.hhu.bsinfo.neutrino.verbs.WorkCompletion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.IntStream;

public class CompletionManager implements CompletionHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(CompletionManager.class);

    public enum Status {
        PENDING, FULFILLED
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private final AtomicReference<Status>[] completions = IntStream.range(0, 1048576)
            .mapToObj(value -> new AtomicReference<>(Status.FULFILLED))
            .toArray(AtomicReference[]::new);

    private final QueueProcessor queueProcessor;

    public CompletionManager(CompletionQueue... completionQueues) {
        queueProcessor = new QueueProcessor(this, completionQueues);
        queueProcessor.start();
    }

    public CompletionManager(CompletionChannel completionChannel) {
        queueProcessor = new QueueProcessor(this, completionChannel);
        queueProcessor.start();
    }

    public void setPending(long id) {
        var index = getIndex(id);

        if (!completions[index].compareAndSet(Status.FULFILLED, Status.PENDING)) {
            LOGGER.error("Completion for request with id {} is not fulfilled yet", id);
        }
    }

    public void await(long id) {
        var index = getIndex(id);

        while (completions[index].get() != Status.FULFILLED) {
            LockSupport.parkNanos(1);
        }
    }

    private void setFulfilled(long id) {
        var index = getIndex(id);
        completions[index].set(Status.FULFILLED);
    }

    private int getIndex(long id) {
        return (int) (id % completions.length);
    }

    @Override
    public void onComplete(long id) {
        setFulfilled(id);
    }

    @Override
    public void onError(long id, WorkCompletion.Status status) {
        LOGGER.error("Request with id {} failed with error [{}]", id, status);
        setFulfilled(id);
    }
}
