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

    /** Concurrent flows bound to the queue. Supports property placeholders. */
    String concurrency() default "";

    /** {@code CLIENT} (default) or {@code AUTO}. CLIENT is required for reply-then-ack. */
    String ackMode() default "CLIENT";

    /** Bean name of a {@link SolaceListenerErrorHandler}. */
    String errorHandler() default "";

    /** Whether to start with the container. Supports property placeholders. */
    String autoStartup() default "true";

    /** Container id, for management and logging. Generated when absent. */
    String id() default "";
}
