package com.solace.samples.requestreply.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * Fails and evicts requests whose deadline has passed.
 *
 * <p>This is what bounds the correlation store. Without it, a reply that never arrives leaks a
 * future and a map entry for the lifetime of the process.
 *
 * <h2>Why {@link #sweep()} never lets an exception escape</h2>
 * The task is scheduled with {@code scheduleWithFixedDelay}, whose documented behaviour is to
 * stop running — forever, silently, with nothing logged and no future execution — the first time
 * it throws. Before this class also bounded the store's own iteration inside the try/catch, an
 * exception anywhere in {@code sweep()} outside the per-entry callback (a bug in a
 * {@link CorrelationStore} implementation, an unexpected error thrown while copying its pending
 * entries) would kill the reaper for the rest of the process's life. From that instant every
 * future request genuinely does leak its future and its map entry, exactly the failure mode this
 * class exists to prevent — it would just do so invisibly instead of on purpose. {@link #sweep()}
 * catching {@code Throwable} is what keeps that failure mode from ever reoccurring, and
 * {@link #lastSweepEpochMs()} is what makes a reaper that somehow still stops running observable
 * rather than silent.
 */
public class TimeoutReaper implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(TimeoutReaper.class);

    private final CorrelationStore store;
    private final Consumer<PendingRequest> onExpiry;
    private final long sweepIntervalMs;
    private final ScheduledExecutorService scheduler;
    private final AtomicLong lastSweepEpochMs = new AtomicLong();
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
        // A sweep "runs" the instant this fires; recorded before start() returns so a health
        // check racing the very first sweep sees a value already, not "never swept".
        lastSweepEpochMs.set(System.currentTimeMillis());
        scheduler.scheduleWithFixedDelay(this::sweep, sweepIntervalMs, sweepIntervalMs, TimeUnit.MILLISECONDS);
    }

    /** Package-visible so tests can sweep deterministically instead of sleeping. */
    void sweep() {
        // Recorded unconditionally, before any of the work below runs, so this timestamp reflects
        // "the scheduler is still ticking" even on a sweep that goes on to fail.
        lastSweepEpochMs.set(System.currentTimeMillis());
        try {
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
        } catch (Throwable ex) {
            // Deliberately Throwable, not RuntimeException: scheduleWithFixedDelay cancels this
            // task forever the moment it throws anything at all, and a reaper that stops running
            // is precisely the unbounded-leak failure mode this class exists to prevent. Nothing
            // below this class's own boundary should ever be allowed to reach the scheduler.
            log.error("Timeout sweep failed; the correlation store was not checked for expired "
                    + "requests this cycle. This is a bug -- please report it -- but the sweep "
                    + "will retry on the next cycle rather than stop.", ex);
        }
    }

    /**
     * Epoch millis of the most recent sweep attempt (successful or not), or {@code 0} if
     * {@link #start()} has never been called.
     *
     * <p>Intended for a health check: {@code System.currentTimeMillis() - lastSweepEpochMs()}
     * staying well above the configured sweep interval means the reaper is no longer running and
     * the correlation store is no longer bounded.
     */
    public long lastSweepEpochMs() { return lastSweepEpochMs.get(); }

    @Override
    public void close() {
        started = false;
        scheduler.shutdownNow();
    }
}
