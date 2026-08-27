package com.solace.samples.requestreply.core;

import com.solace.samples.requestreply.api.RequestReplyMessage;

import java.util.concurrent.CompletableFuture;

/**
 * An in-flight request awaiting its reply.
 */
public final class PendingRequest {

    private final String correlationId;
    private final String requestTopic;
    private final long deadlineEpochMs;
    private final long timeoutMs;
    private final CompletableFuture<RequestReplyMessage> future;

    public PendingRequest(String correlationId, String requestTopic, long deadlineEpochMs,
                          long timeoutMs, CompletableFuture<RequestReplyMessage> future) {
        this.correlationId = correlationId;
        this.requestTopic = requestTopic;
        this.deadlineEpochMs = deadlineEpochMs;
        this.timeoutMs = timeoutMs;
        this.future = future;
    }

    public String getCorrelationId() { return correlationId; }
    public String getRequestTopic() { return requestTopic; }
    public long getDeadlineEpochMs() { return deadlineEpochMs; }
    public long getTimeoutMs() { return timeoutMs; }
    public CompletableFuture<RequestReplyMessage> getFuture() { return future; }

    public boolean isExpired(long nowEpochMs) { return nowEpochMs >= deadlineEpochMs; }
}
