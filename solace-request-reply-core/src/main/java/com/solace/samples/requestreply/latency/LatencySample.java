package com.solace.samples.requestreply.latency;

/**
 * One request's latency, decomposed.
 *
 * <p>A single round-trip number cannot say whether 200ms was a slow publish, a queue backlog,
 * slow business logic or a saturated dispatch thread — and under guaranteed messaging queue
 * dwell is precisely the new failure mode. Splitting it is what turns a measurement into an
 * action: 57ms of dwell says add repliers, 57ms in the handler says look at the database.
 *
 * <p>All durations are monotonic ({@code System.nanoTime}), never differences of wall clocks,
 * which can step backwards under NTP correction and produce negative latencies.
 *
 * <p>{@code dwellNanos} is derived rather than measured: request and reply dwell each span two
 * hosts, so their individual values would inherit clock skew, but the total and the three
 * single-clock segments are exact — which makes their remainder exact too.
 */
public record LatencySample(
        String correlationId,
        Outcome outcome,
        long totalNanos,
        long publishConfirmNanos,
        long handlerNanos,
        long dispatchDelayNanos,
        Long sequence) {

    public enum Outcome { SUCCESS, TIMEOUT, REMOTE_ERROR, PUBLISH_FAILURE }

    /** Combined queue dwell, by subtraction. Never negative even when clocks disagree. */
    public long dwellNanos() {
        return Math.max(0, totalNanos - publishConfirmNanos - handlerNanos - dispatchDelayNanos);
    }

    public long totalMicros() { return totalNanos / 1_000; }
    public long publishConfirmMicros() { return publishConfirmNanos / 1_000; }
    public long handlerMicros() { return handlerNanos / 1_000; }
    public long dispatchDelayMicros() { return dispatchDelayNanos / 1_000; }
    public long dwellMicros() { return dwellNanos() / 1_000; }

    public boolean isSuccess() { return outcome == Outcome.SUCCESS; }
}
