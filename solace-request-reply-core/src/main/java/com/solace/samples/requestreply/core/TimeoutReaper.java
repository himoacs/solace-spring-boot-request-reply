package com.solace.samples.requestreply.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * Fails and evicts requests whose deadline has passed.
 *
 * <p>This is what bounds the correlation store. Without it, a reply that never arrives leaks a
 * future and a map entry for the lifetime of the process.
 */
public class TimeoutReaper implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(TimeoutReaper.class);

    private final CorrelationStore store;
    private final Consumer<PendingRequest> onExpiry;
    private final long sweepIntervalMs;
    private final ScheduledExecutorService scheduler;
    private volatile boolean started;

    public TimeoutReaper(CorrelationStore store, long sweepIntervalMs, Consumer<PendingRequest> onExpiry) {
        this.store = store;
        this.onExpiry = onExpiry;
        this.sweepIntervalMs = Math.max(25, sweepIntervalMs);
        AtomicInteger n = new AtomicInteger();
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "rr-timeout-reaper-" + n.incrementAndGet());
            t.setDaemon(true);
            return t;
        });
    }

    public synchronized void start() {
        if (started) { return; }
        started = true;
        scheduler.scheduleWithFixedDelay(this::sweep, sweepIntervalMs, sweepIntervalMs, TimeUnit.MILLISECONDS);
    }

    /** Package-visible so tests can sweep deterministically instead of sleeping. */
    void sweep() {
        long now = System.currentTimeMillis();
        for (PendingRequest pr : store.pending()) {
            if (!pr.isExpired(now)) { continue; }
            // Atomic remove decides ownership: if a reply took it first, we do nothing.
            store.remove(pr.getCorrelationId()).ifPresent(owned -> {
                try {
                    onExpiry.accept(owned);
                } catch (RuntimeException ex) {
                    log.error("Timeout callback failed for correlationId={}", owned.getCorrelationId(), ex);
                }
            });
        }
    }

    @Override
    public void close() {
        started = false;
        scheduler.shutdownNow();
    }
}
