package com.solace.samples.requestreply.core;

import java.util.Collection;
import java.util.Optional;

/**
 * Registry of in-flight requests, keyed by correlation id.
 *
 * <p>{@link #remove} must be atomic, and that is the whole concurrency design: whoever removes
 * an entry owns completing it. A reply arriving in the same instant as the timeout sweep can
 * therefore never double-complete a future — one of them gets the entry, the other gets empty.
 *
 * <p>State is per-process by design. A distributed implementation would let any instance
 * <em>find</em> a pending request, but not complete it, because the {@code CompletableFuture}
 * lives in one JVM's heap. That is why replies are addressed to a specific instance rather
 * than load-balanced.
 */
public interface CorrelationStore {

    void register(PendingRequest request);

    /** Atomically removes and returns the entry, if it is still present. */
    Optional<PendingRequest> remove(String correlationId);

    Collection<PendingRequest> pending();

    int size();
}
