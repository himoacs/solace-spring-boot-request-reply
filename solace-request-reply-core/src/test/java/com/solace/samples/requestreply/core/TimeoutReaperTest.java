package com.solace.samples.requestreply.core;

import com.solace.samples.requestreply.api.RequestReplyMessage;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Unit tests for {@link TimeoutReaper}, sweeping deterministically via the package-visible
 * {@link TimeoutReaper#sweep()} rather than waiting out real time.
 *
 * <p>No broker, no Spring context: {@link CorrelationStore} is the reaper's entire dependency
 * surface, so a hand-rolled fake is enough to exercise both the happy path and the hardening this
 * class exists for — see {@link #sweepSurvivesAStoreThatThrows()}.
 */
class TimeoutReaperTest {

    @Test
    void sweepEvictsExpiredRequestsAndFiresOnExpiry() {
        InMemoryCorrelationStore store = new InMemoryCorrelationStore();
        CompletableFuture<RequestReplyMessage> future = new CompletableFuture<>();
        PendingRequest expired = new PendingRequest("cid-1", "req/topic",
                System.currentTimeMillis() - 1, 1_000, future);
        store.register(expired);

        List<PendingRequest> expiredSeen = new java.util.ArrayList<>();
        TimeoutReaper reaper = new TimeoutReaper(store, 100, expiredSeen::add);

        reaper.sweep();

        assertThat(expiredSeen).containsExactly(expired);
        assertThat(store.size()).isZero();
        assertThat(store.remove("cid-1")).isEmpty();
    }

    @Test
    void sweepLeavesUnexpiredRequestsAlone() {
        InMemoryCorrelationStore store = new InMemoryCorrelationStore();
        PendingRequest stillWaiting = new PendingRequest("cid-2", "req/topic",
                System.currentTimeMillis() + 60_000, 60_000, new CompletableFuture<>());
        store.register(stillWaiting);

        AtomicInteger expiredCount = new AtomicInteger();
        TimeoutReaper reaper = new TimeoutReaper(store, 100, pr -> expiredCount.incrementAndGet());

        reaper.sweep();

        assertThat(expiredCount.get()).isZero();
        assertThat(store.size()).isEqualTo(1);
    }

    /**
     * The behaviour {@link TimeoutReaper}'s class Javadoc is built around: {@code sweep()} is
     * scheduled with {@code scheduleWithFixedDelay}, which stops running forever, silently, the
     * first time the scheduled task throws. Before this hardening, a {@link CorrelationStore}
     * implementation that threw from {@link CorrelationStore#pending()} — or any other
     * unanticipated exception in {@code sweep()}'s body outside the per-entry callback — would
     * kill the reaper for the rest of the process's life, making the correlation store genuinely
     * unbounded from that point on. This proves {@code sweep()} survives it and keeps ticking.
     */
    @Test
    void sweepSurvivesAStoreThatThrows() {
        CorrelationStore explodingStore = new CorrelationStore() {
            @Override public void register(PendingRequest request) { }
            @Override public Optional<PendingRequest> remove(String correlationId) { return Optional.empty(); }
            @Override public Collection<PendingRequest> pending() {
                throw new IllegalStateException("simulated store failure");
            }
            @Override public int size() { return 0; }
        };
        TimeoutReaper reaper = new TimeoutReaper(explodingStore, 100, pr -> { });

        assertThatCode(reaper::sweep).doesNotThrowAnyException();
        long firstSweep = reaper.lastSweepEpochMs();
        assertThat(firstSweep).isGreaterThan(0);

        // A second sweep after the first one failed proves this isn't just "didn't crash once" --
        // the reaper is still a live, repeatable operation, exactly as scheduleWithFixedDelay
        // expects it to be on every subsequent tick.
        assertThatCode(reaper::sweep).doesNotThrowAnyException();
        assertThat(reaper.lastSweepEpochMs()).isGreaterThanOrEqualTo(firstSweep);
    }

    @Test
    void sweepSurvivesAnOnExpiryCallbackThatThrows() {
        InMemoryCorrelationStore store = new InMemoryCorrelationStore();
        store.register(new PendingRequest("cid-3", "req/topic",
                System.currentTimeMillis() - 1, 1_000, new CompletableFuture<>()));
        TimeoutReaper reaper = new TimeoutReaper(store, 100, pr -> {
            throw new RuntimeException("handler blew up");
        });

        assertThatCode(reaper::sweep).doesNotThrowAnyException();
        // Ownership already transferred via the atomic remove before the callback ran, so the
        // entry is gone from the store regardless of what the callback itself did with it.
        assertThat(store.size()).isZero();
    }
}
