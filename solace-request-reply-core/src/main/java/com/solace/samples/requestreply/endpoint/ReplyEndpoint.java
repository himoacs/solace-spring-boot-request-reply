package com.solace.samples.requestreply.endpoint;

import com.solacesystems.jcsmp.Queue;

import java.util.Map;

/**
 * This instance's private reply endpoint.
 *
 * <p>One queue per requestor instance, because the {@code CompletableFuture} awaiting a reply
 * lives in one JVM's heap and no other instance can complete it. That is the asymmetry at the
 * centre of the design: requests load-balance across a shared queue, replies are addressed to
 * exactly one consumer.
 */
public interface ReplyEndpoint extends AutoCloseable {

    /**
     * Provisions the queue and maps the reply topic onto it.
     *
     * <p>Both in one step, and before any flow binds. A durable queue exists on the broker as
     * soon as it is provisioned, so it can carry a subscription immediately — and doing both
     * up front leaves no window in which the endpoint exists but matches nothing, so a reply
     * published before the flow binds is still spooled rather than lost.
     */
    void establish();

    Queue queue();

    /** Concrete topic this instance publishes as reply-to, per-request placeholders resolved. */
    String replyTopic(Map<String, String> perRequestValues);

    /** Subscription form, with per-request placeholders replaced by {@code *}. */
    String subscription();

    boolean isEstablished();

    @Override
    void close();
}
