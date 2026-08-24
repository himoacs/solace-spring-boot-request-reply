package com.solace.samples.requestreply.endpoint;

import com.solacesystems.jcsmp.Queue;

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
     * Creates or provisions the queue.
     *
     * <p>Does <em>not</em> subscribe: a temporary queue does not exist on the broker until a flow
     * binds to it, so the subscription has to wait. Call {@link #applySubscription()} after binding.
     */
    void establish();

    /**
     * Maps the reply topic onto the queue. Must be called <b>after</b> a flow has bound, because
     * JCSMP rejects a subscription on a queue the broker does not yet hold — "Unknown Queue",
     * subcode 20 — and rejects passing one at bind time, since {@code setNewSubscription} applies
     * to topic endpoints rather than queues.
     */
    void applySubscription();

    /**
     * Re-establishes after a reconnect.
     *
     * <p>Mandatory for a temporary queue, which is destroyed once its linger window expires
     * and recreated <em>without</em> its subscription — a state in which the session is up,
     * the flow is bound, nothing logs an error, and every request times out for ever.
     * Harmless for a durable queue, whose subscription is a broker-side object.
     */
    void reestablish();

    Queue queue();

    /** Concrete topic this instance publishes as reply-to, per-request placeholders resolved. */
    String replyTopic(java.util.Map<String, String> perRequestValues);

    /** Subscription form, with per-request placeholders replaced by {@code *}. */
    String subscription();

    boolean isEstablished();

    @Override
    void close();
}
