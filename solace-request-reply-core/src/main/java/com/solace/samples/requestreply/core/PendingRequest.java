package com.solace.samples.requestreply.core;

import com.solace.samples.requestreply.api.RequestReplyMessage;

import java.util.concurrent.CompletableFuture;

/**
 * An in-flight request awaiting its reply.
 *
 * <p>{@code confirmNanos} is set later than construction, once the broker confirms the publish —
 * which can arrive before or after the reply itself. It lives here rather than in a separate map
 * keyed by correlation id on purpose: a map like that has no natural removal point that lines up
 * with every completion path, and a confirmation arriving after the request was already removed
 * from the correlation store leaks forever. This value dies with the object the store already
 * owns, so there is nothing left over to leak.
 */
public final class PendingRequest {

    private final String correlationId;
    private final String requestTopic;
    private final long deadlineEpochMs;
    private final long timeoutMs;
    private final long startNanos;
    private volatile long confirmNanos;
    private final CompletableFuture<RequestReplyMessage> future;

    public PendingRequest(String correlationId, String requestTopic, long deadlineEpochMs,
                          long timeoutMs, long startNanos,
                          CompletableFuture<RequestReplyMessage> future) {
        this.correlationId = correlationId;
        this.requestTopic = requestTopic;
        this.deadlineEpochMs = deadlineEpochMs;
        this.timeoutMs = timeoutMs;
        this.startNanos = startNanos;
        this.future = future;
    }

    public String getCorrelationId() { return correlationId; }
    public String getRequestTopic() { return requestTopic; }
    public long getDeadlineEpochMs() { return deadlineEpochMs; }
    public long getTimeoutMs() { return timeoutMs; }
    public long getStartNanos() { return startNanos; }
    public CompletableFuture<RequestReplyMessage> getFuture() { return future; }

    /** Time from send to broker acknowledgement, or 0 if the confirmation has not arrived yet. */
    public long getConfirmNanos() { return confirmNanos; }

    /** Set once, from the publish-confirm callback; may run before or after completion. */
    public void setConfirmNanos(long v) { this.confirmNanos = v; }

    public boolean isExpired(long nowEpochMs) { return nowEpochMs >= deadlineEpochMs; }
}
