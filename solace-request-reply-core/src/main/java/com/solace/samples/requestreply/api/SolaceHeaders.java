package com.solace.samples.requestreply.api;

/**
 * Header names used on the wire, named after {@code KafkaHeaders} so the mapping from
 * Spring Kafka is obvious.
 *
 * <p>The first three are <b>not</b> user properties: they map onto native SMF message
 * fields, which is why they survive every hop without the library having to re-stamp them.
 * The {@code rr_} entries are genuine user properties in the message's SDT map.
 */
public final class SolaceHeaders {

    /** Native SMF correlation id. Matches a reply to its request. */
    public static final String CORRELATION_ID = "solace_correlationId";

    /** Native SMF reply-to destination. Where the replier sends the reply. */
    public static final String REPLY_TO = "solace_replyTo";


    /** Error-forwarding flag set by the replier when a handler threw. */
    public static final String ERROR = "rr_error";
    /** Human-readable failure description accompanying {@link #ERROR}. */
    public static final String ERROR_MESSAGE = "rr_error_msg";
    /** Payload content type. */
    public static final String CONTENT_TYPE = "rr_content_type";
    /** Prefix for arbitrary application headers. */
    public static final String USER_PREFIX = "rr_h_";

    /** Replier's clock when it received the request, epoch micros. Cross-host: skew applies. */
    public static final String REQUEST_RECEIVED_AT = "rr_req_recv_us";
    /** Replier's clock when it published the reply, epoch micros. Cross-host: skew applies. */
    public static final String REPLY_SENT_AT = "rr_reply_send_us";
    /** Handler duration in nanos. A duration, so immune to clock skew. */
    public static final String HANDLER_NANOS = "rr_handler_nanos";
    /** Monotonic publisher sequence, for gap and reordering detection under load. */
    public static final String SEQUENCE = "rr_seq";

    private SolaceHeaders() { }
}
