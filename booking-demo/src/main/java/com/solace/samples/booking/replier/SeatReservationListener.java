package com.solace.samples.booking.replier;

import com.solace.samples.booking.domain.BookingRequest;
import com.solace.samples.booking.domain.SeatReservation;
import com.solace.samples.requestreply.api.SolaceHeaders;
import com.solace.samples.requestreply.api.SolaceListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.stereotype.Component;

/**
 * The replier.
 *
 * <p>Reads almost exactly like {@code @KafkaListener} + {@code @SendTo}, which is the point:
 * the queue plays the role of the consumer group, {@code concurrency} the role of a group's
 * parallelism, and bare {@code @SendTo} means "reply to the request's reply-to destination".
 */
@Component
@Profile("!requestor")
public class SeatReservationListener {

    private static final Logger log = LoggerFactory.getLogger(SeatReservationListener.class);

    private final SeatInventoryService inventory;

    public SeatReservationListener(SeatInventoryService inventory) {
        this.inventory = inventory;
    }

    @SolaceListener(
            id = "seatReserve",
            queue = "${solace.request-reply.replier.queue}",
            topics = "${booking.topics.request-subscription}",
            concurrency = "${solace.request-reply.replier.concurrency:4}",
            ackMode = "CLIENT")
    @SendTo
    public SeatReservation reserve(@Payload BookingRequest request,
                                   @Header(SolaceHeaders.CORRELATION_ID) String correlationId,
                                   @Header(value = SolaceHeaders.REPLY_TO, required = false) String replyTo) {

        // A thrown exception becomes an error reply, so "no reply at all" has to be expressed
        // by returning null rather than by throwing.
        if ("timeout".equals(request.simulate())) {
            log.warn("simulate=timeout: dropping the reply for correlationId={} so the requestor "
                    + "genuinely times out", correlationId);
            return null;
        }
        if ("slow-handler".equals(request.simulate())) {
            sleep(2_000);
        }
        if ("remote-error".equals(request.simulate())) {
            throw new IllegalStateException(
                    "No berths available on train " + request.trainNo() + " (simulated)");
        }

        SeatReservation reservation = inventory.reserveOnce(correlationId, request);
        // replyTo is logged because it is the one thing a reader needs to see to believe the
        // per-request placeholder actually works: the train number appears in the reply topic,
        // while the requestor's subscription wildcards that level.
        log.info("{} train={} class={} -> PNR {} {}{} | replyTo={}",
                request.passengerName(), request.trainNo(), request.seatClass().code(),
                reservation.pnr(), reservation.status(),
                reservation.replayed() ? " (replayed)" : "", replyTo);
        return reservation;
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
