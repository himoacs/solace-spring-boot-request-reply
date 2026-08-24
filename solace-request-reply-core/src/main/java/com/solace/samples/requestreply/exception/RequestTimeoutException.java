package com.solace.samples.requestreply.exception;

import java.time.Duration;

/** No correlated reply arrived before the deadline. */
public class RequestTimeoutException extends RequestReplyException {

    private final String correlationId;
    private final String requestTopic;

    public RequestTimeoutException(String correlationId, String requestTopic, Duration timeout) {
        super("No reply for correlationId=" + correlationId + " on '" + requestTopic
                + "' within " + timeout.toMillis() + "ms");
        this.correlationId = correlationId;
        this.requestTopic = requestTopic;
    }

    public String getCorrelationId() { return correlationId; }
    public String getRequestTopic() { return requestTopic; }
}
