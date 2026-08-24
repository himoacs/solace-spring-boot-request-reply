package com.solace.samples.booking.domain;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

/**
 * A seat reservation request.
 *
 * @param zone         railway zone, e.g. {@code nr}. A topic level: the coarsest routing axis.
 * @param trainNo      train number. A topic level, and part of the partition key.
 * @param journeyDate  ISO date of travel. Part of the partition key.
 * @param seatClass    reservation class. Part of the partition key.
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
     * The partition key: the contended inventory row, not the request identity.
     *
     * <p>Granularity is the whole point. Too coarse (zone alone) creates hot partitions; too
     * fine (a request id) hashes randomly, which restores the very race partitioning was meant
     * to remove while looking entirely correct. Train, date and class together is the row being
     * locked.
     */
    public String partitionKey() {
        return trainNo + "-" + journeyDate + "-" + seatClass.code();
    }

    public int seatsOrOne() { return Math.max(1, passengers); }
}
