package com.solace.samples.requestreply.core;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/** {@link CorrelationStore} over a {@link ConcurrentHashMap}. */
public class InMemoryCorrelationStore implements CorrelationStore {

    private final ConcurrentHashMap<String, PendingRequest> byId = new ConcurrentHashMap<>();

    @Override
    public void register(PendingRequest request) {
        byId.put(request.getCorrelationId(), request);
    }

    @Override
    public Optional<PendingRequest> remove(String correlationId) {
        return correlationId == null ? Optional.empty() : Optional.ofNullable(byId.remove(correlationId));
    }

    @Override
    public Collection<PendingRequest> pending() { return List.copyOf(byId.values()); }

    @Override
    public int size() { return byId.size(); }
}
