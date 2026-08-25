package com.solace.samples.requestreply.api;

import java.time.Duration;

/**
 * Requestor-side API, named after Spring Kafka's {@code ReplyingKafkaTemplate}.
 *
 * <p>Solace messages have no key, so the reply type moves to the method rather than the
 * class, matching Spring Kafka's own {@code ParameterizedTypeReference} overloads.
 */
public interface ReplyingSolaceTemplate {

    /**
     * Publishes {@code request} to {@code topic} and returns a future for the reply.
     *
     * @param topic     request topic
     * @param payload   request body; serialized by the configured converter
     * @param replyType type to deserialize the reply into
     * @param timeout   how long to wait before failing with {@code RequestTimeoutException}
     */
    <T, R> RequestReplyFuture<R> sendAndReceive(String topic, T payload,
                                                Class<R> replyType, Duration timeout);

    /** As above, using the configured default timeout. */
    default <T, R> RequestReplyFuture<R> sendAndReceive(String topic, T payload, Class<R> replyType) {
        return sendAndReceive(topic, payload, replyType, defaultReplyTimeout());
    }

    /**
     * Raw form, for callers that manage their own serialization and headers.
     *
     * <p>Setting {@code request.correlationId} explicitly reuses it rather than generating one,
     * which is how a redelivery can be reproduced on demand: the same id twice must yield one
     * reservation and two identical replies.
     */
    RequestReplyFuture<RequestReplyMessage> sendAndReceive(String topic, RequestReplyMessage request,
                                                          Duration timeout);

    /** Typed form with an explicit correlation id, for replaying a request. */
    <T, R> RequestReplyFuture<R> sendAndReceive(String topic, T payload, Class<R> replyType,
                                               Duration timeout, String correlationId);

    Duration defaultReplyTimeout();

    /**
     * Blocks until the reply endpoint is provisioned, subscribed and bound.
     *
     * <p>The analogue of Spring Kafka's {@code waitForAssignment}, and it addresses the same
     * race: publishing before the reply path exists means the reply has nowhere to land.
     *
     * @return true if ready within {@code timeout}
     */
    boolean waitForReplyEndpoint(Duration timeout);

    /** The reply topic this instance publishes as its reply-to. */
    String replyTopic();
}
