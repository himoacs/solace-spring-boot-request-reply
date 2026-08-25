package com.solace.samples.booking.replier;

import com.solace.samples.booking.domain.BookingRequest;
import com.solace.samples.booking.domain.SeatReservation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Seat inventory, with the two properties that make guaranteed messaging safe for bookings.
 *
 * <h2>Idempotency</h2>
 * Guaranteed delivery is at-least-once, not exactly-once. On a non-exclusive queue an
 * unacknowledged message is redelivered to another consumer, so a replier that reserves a seat
 * and then dies before acknowledging will see the same request again — and naively reserve a
 * second seat. The correlation id is therefore recorded <em>with</em> the reservation, and a
 * repeat returns the original reply rather than doing the work twice.
 *
 * <p>In a real service that record and the reservation are one database transaction under a
 * unique constraint. Here it is a map, which demonstrates the shape without pulling in a
 * database.
 *
 * <h2>Concurrency</h2>
 * A flat non-exclusive queue load-balances across every bound flow, so two requests for the last
 * berth on one train can be handled at the same moment, by different threads or different
 * instances. The inventory row is therefore mutated under a lock — {@code compute} on a per-row
 * key — because the invariant belongs here rather than in how messages happen to be distributed.
 */
@Service
public class SeatInventoryService {

    private static final Logger log = LoggerFactory.getLogger(SeatInventoryService.class);
    private static final int SEATS_PER_ROW = 72;
    private static final String[] COACHES = {"B1", "B2", "B3", "B4", "A1", "S1", "S2"};

    /** correlationId -> the reply already produced for it. The idempotency guard. */
    private final Map<String, SeatReservation> byCorrelationId = new ConcurrentHashMap<>();

    /** inventory row -> seats already allotted on that train/date/class. */
    private final Map<String, AtomicInteger> allotted = new ConcurrentHashMap<>();

    /**
     * Reserves seats at most once for {@code correlationId}.
     *
     * @return a fresh reservation, or the previously produced one with {@code replayed=true}
     */
    public SeatReservation reserveOnce(String correlationId, BookingRequest request) {
        SeatReservation existing = byCorrelationId.get(correlationId);
        if (existing != null) {
            log.info("Duplicate delivery for correlationId={} — returning the original PNR {} "
                    + "instead of reserving again", correlationId, existing.pnr());
            return new SeatReservation(existing.pnr(), existing.status(), existing.coach(),
                    existing.berths(), existing.trainNo(), true);
        }
        // computeIfAbsent gives at-most-once even if two threads race the same correlation id.
        return byCorrelationId.computeIfAbsent(correlationId, id -> reserve(request));
    }

    private SeatReservation reserve(BookingRequest request) {
        String row = request.inventoryRow();
        int seats = request.seatsOrOne();
        AtomicInteger counter = allotted.computeIfAbsent(row, k -> new AtomicInteger());

        int firstSeat = counter.getAndAdd(seats) + 1;
        boolean confirmed = firstSeat + seats - 1 <= SEATS_PER_ROW;

        String coach = COACHES[Math.floorMod(row.hashCode(), COACHES.length)];
        StringBuilder berths = new StringBuilder();
        for (int i = 0; i < seats; i++) {
            if (i > 0) { berths.append(','); }
            berths.append(firstSeat + i);
        }

        SeatReservation reservation = new SeatReservation(
                pnr(request, firstSeat),
                confirmed ? "CONFIRMED" : "WAITLISTED",
                confirmed ? coach : "-",
                confirmed ? berths.toString() : "WL/" + (firstSeat - SEATS_PER_ROW),
                request.trainNo(),
                false);
        log.debug("Reserved {} seat(s) on {}: {}", seats, row, reservation);
        return reservation;
    }

    private static String pnr(BookingRequest r, int firstSeat) {
        long h = Math.abs((long) (r.inventoryRow() + firstSeat).hashCode());
        return String.format("%010d", h % 10_000_000_000L);
    }

    /** Distinct reservations made, ignoring redeliveries. Used by tests and diagnostics. */
    public int reservationCount() { return byCorrelationId.size(); }

    public int seatsAllotted(String inventoryRow) {
        AtomicInteger c = allotted.get(inventoryRow);
        return c == null ? 0 : c.get();
    }
}
