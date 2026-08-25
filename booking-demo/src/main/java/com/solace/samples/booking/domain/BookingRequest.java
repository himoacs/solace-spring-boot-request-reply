package com.solace.samples.booking.domain;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

/**
 * A seat reservation request.
 *
 * @param zone         railway zone, e.g. {@code nr}. A topic level: the coarsest routing axis.
 * @param trainNo      train number. A topic level, and part of the inventory row.
 * @param journeyDate  ISO date of travel. Part of the inventory row.
 * @param seatClass    reservation class. Part of the inventory row.
 * @param passengerName passenger
 * @param passengers   seats requested
 * @param simulate     test hook: {@code timeout}, {@code remote-error}, {@code slow-handler}.
 *                     Present so every branch of the failure taxonomy is reproducible without
 *                     having to break the broker.
 */
public record BookingRequest(
        @NotBlank String zone,
        @NotBlank String trainNo,
        @NotBlank String journeyDate,
        @NotNull SeatClass seatClass,
        @NotBlank String passengerName,
        int passengers,
        String simulate) {

    /**
     * The contended inventory row: the seats that two simultaneous bookings compete for.
     *
     * <p>Train, date and class together, because that is the granularity at which seats are
     * actually allotted. It is the key the inventory is locked on, and the identity a
     * reservation is derived from — not the identity of the request, of which there is one per
     * caller and which would therefore protect nothing.
     */
    public String inventoryRow() {
        return trainNo + "-" + journeyDate + "-" + seatClass.code();
    }

    public int seatsOrOne() { return Math.max(1, passengers); }
}
