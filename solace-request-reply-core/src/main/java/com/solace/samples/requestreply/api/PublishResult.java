package com.solace.samples.requestreply.api;

/**
 * Outcome of the publish itself, distinct from the outcome of the request.
 *
 * <p>The analogue of Spring Kafka's {@code SendResult}: it completes when the broker has
 * acknowledged that the message is <em>spooled</em>, which for a PERSISTENT publish happens
 * well after {@code producer.send()} returns.
 *
 * @param correlationId the request this publish belongs to
 * @param topic         the destination it was published to
 * @param confirmNanos  time from send to broker acknowledgement, nanoseconds
 */
public record PublishResult(String correlationId, String topic, long confirmNanos) {

    public long confirmMicros() { return confirmNanos / 1_000; }
}
