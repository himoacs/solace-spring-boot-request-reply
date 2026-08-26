package com.solace.samples.requestreply.api;

/**
 * Thrown by a {@link SolaceListener} handler to ask for this request to be tried again, instead
 * of answered with an error.
 *
 * <p>Every other exception a handler throws becomes an error reply: the requestor is told
 * immediately rather than waiting out its timeout, and the request is acknowledged, because the
 * work is considered done — badly, but done. That is right for a failure that will not change on
 * a second attempt, such as a validation error or a business rule violation. It is wrong for one
 * that might succeed a moment later, such as a database connection blip: turning that into a
 * permanent failure reply is how a transient hiccup becomes a booking the customer is told never
 * happened.
 *
 * <p>Throwing this type instead settles the underlying message {@code FAILED} rather than
 * acknowledging it. That is an active request to the broker to redeliver, not merely leaving the
 * message unacknowledged — an unacknowledged CLIENT-ack message only redelivers if the flow
 * disconnects, which a healthy connection never does. The broker increments the queue's delivery
 * count immediately and redelivers, moving the message to the dead message queue once
 * {@code replier.provision.max-redelivery} is reached, the same as any other exhausted request.
 *
 * <p>No reply is published for this path, and the configured {@link SolaceListenerErrorHandler}
 * is not consulted: retry is a delivery decision the handler made deliberately by throwing this
 * type, not something for a reply-shaping component to override.
 *
 * <p><b>This does not make the original caller's call succeed.</b> The requestor's future still
 * runs out on its own {@code request.timeout} regardless of how many redelivery attempts follow;
 * what retry buys is that the underlying work is not silently converted into a permanent failure
 * the moment a transient error is hit. Redelivery is also bounded by the request's own TTL, which
 * defaults to {@code request.timeout} via {@code ttl-matches-timeout} — a message can expire off
 * the queue mid-retry before {@code max-redelivery} is reached at all. Raise {@code
 * request.timeout} if the handler needs real retry budget rather than one fast attempt.
 */
public class RetryableHandlerException extends RuntimeException {

    public RetryableHandlerException(String message) { super(message); }

    public RetryableHandlerException(String message, Throwable cause) { super(message, cause); }
}
