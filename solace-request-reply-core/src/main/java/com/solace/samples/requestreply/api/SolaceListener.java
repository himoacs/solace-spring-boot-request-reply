package com.solace.samples.requestreply.api;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a method as a request handler bound to a queue, after {@code @KafkaListener}.
 *
 * <p>Combine with Spring's own {@code @SendTo} to publish the return value as the reply.
 * With no value, {@code @SendTo} routes to the request's reply-to destination, which is the
 * same default Spring Kafka uses.
 *
 * <pre>{@code
 * @SolaceListener(queue = "q.cris.booking.seatReserve",
 *                 topics = "cris/booking/seatReserve/request/v1/>",
 *                 concurrency = "10")
 * @SendTo
 * public SeatReservation reserve(@Payload BookingRequest req,
 *                                @Header(SolaceHeaders.CORRELATION_ID) String correlationId) {
 *     return service.reserveOnce(correlationId, req);
 * }
 * }</pre>
 *
 * <p><b>The queue is the consumer group.</b> A non-exclusive queue with several bound flows
 * is the Solace equivalent of a Kafka consumer group: every instance binds the same queue
 * and the broker load-balances across them. This is what stops the fan-out that a direct
 * topic subscription would cause, where every replier would receive — and act on — every
 * request.
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface SolaceListener {

    /** Queue to bind. Required: this is the consumer group. */
    String queue();

    /**
     * Topic subscriptions to map onto the queue. Wildcards are broker-side, so
     * {@code .../v1/>} filters without the application seeing unwanted messages.
     */
    String[] topics() default {};

    /**
     * Flows bound to the queue, and the size of this listener's own handler thread pool.
     *
     * <p>Both, because JCSMP dispatches every flow in a session on one shared thread — flows
     * alone buy no parallelism. The pool is where handlers actually run concurrently, and it
     * belongs to this listener alone, so a slow handler here cannot starve a different
     * {@code @SolaceListener} in the same process. Supports property placeholders.
     */
    String concurrency() default "";

    /**
     * {@code CLIENT} (default) or {@code AUTO}.
     *
     * <p>{@code CLIENT}: this container acknowledges the request itself, only after the reply
     * has been published <em>and</em> the broker has confirmed it. A crash before that point
     * produces a redelivery instead of a silently lost request; a handler that throws
     * {@link RetryableHandlerException} gets one too, on demand. Required for a handler that
     * publishes a reply ({@code @SendTo}, or a non-void return type), and the right default
     * for everything else too.
     *
     * <p>{@code AUTO}: JCSMP acknowledges for you, right after the handler returns — which
     * means the handler has to run inline, on JCSMP's one dispatch thread shared by every
     * listener in the process, rather than on this container's own pool. A slow AUTO handler
     * therefore blocks every other listener and every reply for as long as it runs. It also has
     * no failure path: JCSMP's settlement outcomes — what makes an active redelivery possible —
     * are a CLIENT-ack-only capability, confirmed against Solace's own documentation, so a
     * handler that throws, including {@code RetryableHandlerException}, cannot stop the message
     * from being acknowledged anyway. And if the handler publishes a reply, the ack only means
     * the publish was started, not that the broker confirmed it. Reasonable for a fast handler
     * with no reply, where an occasional silently-lost failure is acceptable; not a substitute
     * for {@code CLIENT} wherever a reply, retry, or firm delivery guarantee matters.
     */
    String ackMode() default "CLIENT";

    /**
     * Bean name of a {@link SolaceListenerErrorHandler} for this listener.
     *
     * <p>Left empty, the single {@code SolaceListenerErrorHandler} bean in the context applies, if
     * there is exactly one. Name a bean here when several listeners need to fail differently.
     */
    String errorHandler() default "";

    /** Whether to start with the container. Supports property placeholders. */
    String autoStartup() default "true";

    /** Container id, for management and logging. Generated when absent. */
    String id() default "";
}
