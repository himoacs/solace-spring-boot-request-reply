package com.solace.samples.booking.web;

import com.solace.samples.booking.domain.BookingRequest;
import com.solace.samples.booking.domain.SeatReservation;
import com.solace.samples.requestreply.api.ReplyingSolaceTemplate;
import com.solace.samples.requestreply.api.RequestReplyFuture;
import com.solace.samples.requestreply.exception.RemoteErrorException;
import com.solace.samples.requestreply.exception.RequestTimeoutException;
import com.solace.samples.requestreply.exception.TransportException;
import jakarta.validation.Valid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * REST facade over the request/reply round trip.
 *
 * <p>Demonstrates the two-stage future in the one place a reader will actually look: the send
 * future is awaited separately so a rejected publish is reported as a publish failure
 * immediately, rather than surfacing five seconds later as a reply timeout that points at the
 * wrong half of the system.
 */
@RestController
@RequestMapping("/api/bookings")
public class BookingController {

    private static final Logger log = LoggerFactory.getLogger(BookingController.class);

    /**
     * An ObjectProvider, not the template itself, because a replier-only process has no
     * requestor side at all: {@code reply.enabled=false} removes the template bean. The
     * endpoint stays mapped so that calling it on the wrong pod explains itself rather than
     * returning a bare 404.
     */
    private final ObjectProvider<ReplyingSolaceTemplate> templateProvider;
    private final String requestTopicPattern;

    public BookingController(ObjectProvider<ReplyingSolaceTemplate> templateProvider,
                             @Value("${booking.topics.request-pattern}") String requestTopicPattern) {
        this.templateProvider = templateProvider;
        this.requestTopicPattern = requestTopicPattern;
    }

    @PostMapping
    public ResponseEntity<Map<String, Object>> book(@Valid @RequestBody BookingRequest request) {
        return execute(request);
    }

    private ResponseEntity<Map<String, Object>> execute(BookingRequest request) {
        ReplyingSolaceTemplate template = templateProvider.getIfAvailable();
        if (template == null) {
            Map<String, Object> body = new LinkedHashMap<>();
            body.put("error", "not-a-requestor");
            body.put("detail", "This process runs replier-only "
                    + "(solace.request-reply.reply.enabled=false), so it has no reply queue and "
                    + "cannot send a request. Send bookings to a requestor instance instead.");
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(body);
        }

        String topic = requestTopicPattern
                .replace("{zone}", request.zone())
                .replace("{trainNo}", request.trainNo());

        long start = System.nanoTime();
        RequestReplyFuture<SeatReservation> future = template.sendAndReceive(
                topic, request, SeatReservation.class, template.defaultReplyTimeout());

        // Stage one: did the broker accept and spool the request?
        long confirmNanos;
        try {
            confirmNanos = future.getSendFuture().get(5, TimeUnit.SECONDS).confirmNanos();
        } catch (ExecutionException e) {
            return problem(HttpStatus.BAD_GATEWAY, "publish-failed",
                    "The broker did not accept the request: " + rootMessage(e), topic, null);
        } catch (TimeoutException e) {
            return problem(HttpStatus.GATEWAY_TIMEOUT, "publish-unconfirmed",
                    "The broker did not acknowledge the request within 5s", topic, null);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return problem(HttpStatus.SERVICE_UNAVAILABLE, "interrupted", "Interrupted", topic, null);
        }

        // Stage two: did anybody answer?
        try {
            SeatReservation reservation = future.get(
                    template.defaultReplyTimeout().toMillis() + 1_000, TimeUnit.MILLISECONDS);
            long total = System.nanoTime() - start;

            Map<String, Object> body = new LinkedHashMap<>();
            body.put("reservation", reservation);
            body.put("latency", Map.of(
                    "totalMicros", total / 1_000,
                    "publishConfirmMicros", confirmNanos / 1_000));
            body.put("requestTopic", topic);
            body.put("inventoryRow", request.inventoryRow());
            body.put("replyTopicPattern", template.replyTopicPattern());
            return ResponseEntity.ok(body);

        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RequestTimeoutException) {
                return problem(HttpStatus.GATEWAY_TIMEOUT, "reply-timeout",
                        "The request was spooled but no replier answered in time", topic, confirmNanos);
            }
            if (cause instanceof RemoteErrorException re) {
                return problem(HttpStatus.UNPROCESSABLE_ENTITY, "remote-error",
                        re.getRemoteMessage(), topic, confirmNanos);
            }
            if (cause instanceof TransportException) {
                return problem(HttpStatus.BAD_GATEWAY, "transport-error",
                        rootMessage(e), topic, confirmNanos);
            }
            log.error("Unexpected failure for topic {}", topic, e);
            return problem(HttpStatus.INTERNAL_SERVER_ERROR, "error", rootMessage(e), topic, confirmNanos);

        } catch (TimeoutException e) {
            return problem(HttpStatus.GATEWAY_TIMEOUT, "reply-timeout",
                    "No reply within the deadline", topic, confirmNanos);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return problem(HttpStatus.SERVICE_UNAVAILABLE, "interrupted", "Interrupted", topic, confirmNanos);
        }
    }

    private static ResponseEntity<Map<String, Object>> problem(HttpStatus status, String code,
                                                               String detail, String topic,
                                                               Long confirmNanos) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("error", code);
        body.put("detail", detail);
        body.put("requestTopic", topic);
        // Present even on failure: it is what distinguishes "never landed" from "nobody answered".
        body.put("publishConfirmed", confirmNanos != null);
        if (confirmNanos != null) { body.put("publishConfirmMicros", confirmNanos / 1_000); }
        return ResponseEntity.status(status).body(body);
    }

    private static String rootMessage(Throwable t) {
        Throwable c = t;
        while (c.getCause() != null) { c = c.getCause(); }
        return c.getMessage() == null ? c.getClass().getSimpleName() : c.getMessage();
    }
}
