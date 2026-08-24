package com.solace.samples.requestreply.core;

import com.solace.samples.requestreply.api.RequestReplyMessage;

import java.util.concurrent.CompletableFuture;

/**
 * An in-flight request awaiting its reply.
 *
 * <p>Carries the captured observability context alongside the future. That is not incidental:
 * {@code future.complete()} runs its dependents on whichever thread called it — the JCSMP
 * dispatch thread, whose current context is the <em>reply's</em> receive context, not the
 * request's. Restoring the captured context on completion is what keeps a trace's causality
 * correct rather than merely connected.
 */
public final class PendingRequest {

    private final String correlationId;
    private final String requestTopic;
    private final long deadlineEpochMs;
    private final long timeoutMs;
    private final long startNanos;
    private final long publishConfirmNanosHolder;
    private final CompletableFuture<RequestReplyMessage> future;
    private final Object tracingContext;

    public PendingRequest(String correlationId, String requestTopic, long deadlineEpochMs,
                          long timeoutMs, long startNanos,
                          CompletableFuture<RequestReplyMessage> future, Object tracingContext) {
        this.correlationId = correlationId;
        this.requestTopic = requestTopic;
        this.deadlineEpochMs = deadlineEpochMs;
        this.timeoutMs = timeoutMs;
        this.startNanos = startNanos;
        this.publishConfirmNanosHolder = 0L;
        this.future = future;
        this.tracingContext = tracingContext;
    }

    public String getCorrelationId() { return correlationId; }
    public String getRequestTopic() { return requestTopic; }
    public long getDeadlineEpochMs() { return deadlineEpochMs; }
    public long getTimeoutMs() { return timeoutMs; }
    public long getStartNanos() { return startNanos; }
    public CompletableFuture<RequestReplyMessage> getFuture() { return future; }
    public Object getTracingContext() { return tracingContext; }

    public boolean isExpired(long nowEpochMs) { return nowEpochMs >= deadlineEpochMs; }
}
