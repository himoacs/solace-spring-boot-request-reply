package com.solace.samples.requestreply.exception;

/**
 * Rejected before publishing: {@code solace.request-reply.request.max-pending} in-flight
 * requests are already registered on this instance.
 *
 * <p>Raised instead of registering another {@code PendingRequest}, so a caller that outruns the
 * replier gets a fast, typed failure it can retry or shed load on, rather than the request being
 * accepted silently into an ever-growing correlation store.
 */
public class RequestBackpressureException extends RequestReplyException {

    private final String requestTopic;
    private final int maxPending;

    public RequestBackpressureException(String requestTopic, int maxPending) {
        super("Rejecting request to '" + requestTopic + "': " + maxPending + " requests already "
                + "in flight (solace.request-reply.request.max-pending)");
        this.requestTopic = requestTopic;
        this.maxPending = maxPending;
    }

    public String getRequestTopic() { return requestTopic; }
    public int getMaxPending() { return maxPending; }
}
