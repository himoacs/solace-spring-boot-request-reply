package com.solace.samples.booking.domain;

/**
 * The reply.
 *
 * @param pnr           reservation reference
 * @param status        CONFIRMED or WAITLISTED
 * @param coach         allotted coach
 * @param berths        allotted berths
 * @param trainNo       echoed for correlation in logs
 * @param replayed      true when this reply came from the idempotency cache rather than a
 *                      fresh reservation — the visible signal that a redelivery did not
 *                      double-book
 */
public record SeatReservation(
        String pnr,
        String status,
        String coach,
        String berths,
        String trainNo,
        boolean replayed) {
}
