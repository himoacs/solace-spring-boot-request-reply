package com.solace.samples.requestreply.api;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Transport-neutral request or reply. The payload is opaque bytes; serialization is the
 * application's business.
 *
 * <p>Correlation and routing fields are managed by the library. Application code sets a
 * payload and, optionally, headers.
 */
public final class RequestReplyMessage {

    private byte[] payload;
    private String correlationId;
    private String replyTo;
    private String contentType = "application/json";
    private String partitionKey;
    private Long sequence;

    private boolean error;
    private String errorMessage;
    /**
     * Whether the broker may move this message to the dead message queue rather than deleting
     * it once it exhausts redelivery or expires. Off by default, matching JCSMP: the request
     * and reply paths opt in from configuration, while the reply-path canary deliberately does
     * not, since it carries a TTL and would otherwise dead-letter a probe on every reconnect.
     */
    private boolean dmqEligible;

    private final Map<String, String> headers = new LinkedHashMap<>();

    public RequestReplyMessage() { }

    public RequestReplyMessage(byte[] payload) { this.payload = payload; }

    public static RequestReplyMessage of(byte[] payload) { return new RequestReplyMessage(payload); }

    public static RequestReplyMessage of(String text) {
        return new RequestReplyMessage(text == null ? null : text.getBytes(StandardCharsets.UTF_8));
    }

    public byte[] getPayload() { return payload; }

    public RequestReplyMessage setPayload(byte[] payload) { this.payload = payload; return this; }

    public String getPayloadAsString() {
        return payload == null ? null : new String(payload, StandardCharsets.UTF_8);
    }

    public String getCorrelationId() { return correlationId; }

    public RequestReplyMessage setCorrelationId(String v) { this.correlationId = v; return this; }

    public String getReplyTo() { return replyTo; }

    public RequestReplyMessage setReplyTo(String v) { this.replyTo = v; return this; }

    public String getContentType() { return contentType; }

    public RequestReplyMessage setContentType(String v) { this.contentType = v; return this; }

    public String getPartitionKey() { return partitionKey; }

    public RequestReplyMessage setPartitionKey(String v) { this.partitionKey = v; return this; }

    public Long getSequence() { return sequence; }

    public RequestReplyMessage setSequence(Long v) { this.sequence = v; return this; }

    public boolean isDmqEligible() { return dmqEligible; }

    public RequestReplyMessage setDmqEligible(boolean v) { this.dmqEligible = v; return this; }

    public boolean isError() { return error; }

    public String getErrorMessage() { return errorMessage; }

    /** Marks this message as a failure reply carrying {@code description}. */
    public RequestReplyMessage asError(String description) {
        this.error = true;
        this.errorMessage = description;
        return this;
    }

    public Map<String, String> getHeaders() { return Collections.unmodifiableMap(headers); }

    public RequestReplyMessage addHeader(String key, String value) {
        if (key != null && value != null) { headers.put(key, value); }
        return this;
    }

    public String getHeader(String key) { return headers.get(key); }

    /**
     * A reply skeleton for this request: correlation id copied, destination taken from
     * this request's reply-to.
     */
    public RequestReplyMessage newReply() {
        return new RequestReplyMessage()
                .setCorrelationId(this.correlationId)
                .setReplyTo(this.replyTo)
                .setContentType(this.contentType);
    }

    @Override
    public String toString() {
        return "RequestReplyMessage{correlationId=" + correlationId
                + ", replyTo=" + replyTo
                + ", partitionKey=" + partitionKey
                + ", error=" + error
                + ", bytes=" + (payload == null ? 0 : payload.length) + '}';
    }
}
