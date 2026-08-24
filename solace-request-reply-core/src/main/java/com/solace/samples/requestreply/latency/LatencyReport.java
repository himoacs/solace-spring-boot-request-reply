package com.solace.samples.requestreply.latency;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Exact percentiles over a completed run.
 *
 * <p>Deliberately not a histogram or a sketch. Those exist because a continuous production
 * stream cannot retain every sample; a bounded test can. 100,000 longs is 800 KB and sorts in
 * about ten milliseconds, so keeping every sample is both simpler <em>and</em> more accurate
 * than interpolating from buckets — no estimation error at all in the tail, which is the part
 * anyone actually asks about.
 */
public final class LatencyReport {

    /** Load-generation shape. Reported, because it changes what the numbers mean. */
    public enum Mode {
        /**
         * Bounded in-flight requests, each waiting for its reply. Measures <em>service time at
         * that concurrency</em>. Structurally under-samples slow periods — when the system slows,
         * the generator issues fewer requests — so the tail flatters reality.
         */
        CLOSED_LOOP,
        /**
         * Fixed arrival rate regardless of replies, measured from the <em>intended</em> send time
         * so generator queueing counts against the result. Measures latency at an arrival rate.
         */
        OPEN_LOOP
    }

    private final Mode mode;
    private final int requested;
    private final int concurrency;
    private final int warmupDiscarded;
    private final double wallClockSeconds;
    private final Map<LatencySample.Outcome, Integer> outcomes;
    private final long[] total;
    private final long[] dwell;
    private final long[] handler;
    private final long[] publishConfirm;
    private final long[] dispatch;
    private final long sequenceGaps;
    private final long outOfOrder;

    private LatencyReport(Builder b) {
        this.mode = b.mode;
        this.requested = b.requested;
        this.concurrency = b.concurrency;
        this.warmupDiscarded = b.warmupDiscarded;
        this.wallClockSeconds = b.wallClockSeconds;
        this.outcomes = b.outcomes;
        this.total = b.total;
        this.dwell = b.dwell;
        this.handler = b.handler;
        this.publishConfirm = b.publishConfirm;
        this.dispatch = b.dispatch;
        this.sequenceGaps = b.sequenceGaps;
        this.outOfOrder = b.outOfOrder;
    }

    /** Builds a report from raw samples, sorting each series once. */
    public static LatencyReport of(List<LatencySample> samples, Mode mode, int requested,
                                  int concurrency, int warmupDiscarded, double wallClockSeconds) {
        Builder b = new Builder();
        b.mode = mode;
        b.requested = requested;
        b.concurrency = concurrency;
        b.warmupDiscarded = warmupDiscarded;
        b.wallClockSeconds = wallClockSeconds;

        b.outcomes = new EnumMap<>(LatencySample.Outcome.class);
        for (LatencySample.Outcome o : LatencySample.Outcome.values()) { b.outcomes.put(o, 0); }
        for (LatencySample s : samples) { b.outcomes.merge(s.outcome(), 1, Integer::sum); }

        // Latency series cover successful requests only; failures are counted, not timed, so a
        // run where five percent timed out cannot look fast. The outcome table is right beside
        // the percentiles for exactly that reason.
        List<LatencySample> ok = samples.stream().filter(LatencySample::isSuccess).toList();
        b.total = sortedMicros(ok, LatencySample::totalMicros);
        b.dwell = sortedMicros(ok, LatencySample::dwellMicros);
        b.handler = sortedMicros(ok, LatencySample::handlerMicros);
        b.publishConfirm = sortedMicros(ok, LatencySample::publishConfirmMicros);
        b.dispatch = sortedMicros(ok, LatencySample::dispatchDelayMicros);

        long[] seq = ok.stream().map(LatencySample::sequence).filter(java.util.Objects::nonNull)
                .mapToLong(Long::longValue).toArray();
        b.outOfOrder = countOutOfOrder(seq);
        b.sequenceGaps = countGaps(seq);
        return new LatencyReport(b);
    }

    private static long[] sortedMicros(List<LatencySample> in,
                                       java.util.function.ToLongFunction<LatencySample> f) {
        long[] out = in.stream().mapToLong(f).toArray();
        java.util.Arrays.sort(out);
        return out;
    }

    /** Arrivals out of publish order — non-zero on a flat non-exclusive queue, by design. */
    private static long countOutOfOrder(long[] arrivalOrder) {
        long n = 0;
        for (int i = 1; i < arrivalOrder.length; i++) {
            if (arrivalOrder[i] < arrivalOrder[i - 1]) { n++; }
        }
        return n;
    }

    /** Missing sequence numbers, i.e. actual message loss. Should always be zero. */
    private static long countGaps(long[] seq) {
        if (seq.length == 0) { return 0; }
        long[] s = seq.clone();
        java.util.Arrays.sort(s);
        return (s[s.length - 1] - s[0] + 1) - s.length;
    }

    /** Exact percentile: index into the sorted series, no interpolation. */
    public static long percentile(long[] sorted, double p) {
        if (sorted.length == 0) { return 0; }
        int idx = (int) Math.ceil(p * sorted.length) - 1;
        return sorted[Math.max(0, Math.min(sorted.length - 1, idx))];
    }

    public Mode mode() { return mode; }
    public int requested() { return requested; }
    public int concurrency() { return concurrency; }
    public int warmupDiscarded() { return warmupDiscarded; }
    public double wallClockSeconds() { return wallClockSeconds; }
    public int completed() { return outcomes.values().stream().mapToInt(Integer::intValue).sum(); }
    public double throughput() { return wallClockSeconds <= 0 ? 0 : completed() / wallClockSeconds; }
    public Map<LatencySample.Outcome, Integer> outcomes() { return Map.copyOf(outcomes); }
    public long[] total() { return total; }
    public long[] dwell() { return dwell; }
    public long[] handler() { return handler; }
    public long[] publishConfirm() { return publishConfirm; }
    public long[] dispatch() { return dispatch; }
    public long sequenceGaps() { return sequenceGaps; }
    public long outOfOrder() { return outOfOrder; }

    /** JSON-friendly view for the REST endpoint. */
    public Map<String, Object> toMap() {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("mode", mode);
        out.put("requested", requested);
        out.put("concurrency", concurrency);
        out.put("warmupDiscarded", warmupDiscarded);
        out.put("wallClockSeconds", Math.round(wallClockSeconds * 10) / 10.0);
        out.put("throughputPerSecond", Math.round(throughput()));
        Map<String, Integer> oc = new LinkedHashMap<>();
        outcomes.forEach((k, v) -> oc.put(k.name().toLowerCase(), v));
        out.put("outcomes", oc);
        out.put("totalMicros", series(total));
        out.put("dwellMicros", series(dwell));
        out.put("handlerMicros", series(handler));
        out.put("publishConfirmMicros", series(publishConfirm));
        out.put("sequenceGaps", sequenceGaps);
        out.put("outOfOrder", outOfOrder);
        return out;
    }

    private static Map<String, Long> series(long[] s) {
        Map<String, Long> m = new LinkedHashMap<>();
        m.put("p50", percentile(s, 0.50));
        m.put("p90", percentile(s, 0.90));
        m.put("p95", percentile(s, 0.95));
        m.put("p99", percentile(s, 0.99));
        m.put("p999", percentile(s, 0.999));
        m.put("max", s.length == 0 ? 0 : s[s.length - 1]);
        return m;
    }

    private static final class Builder {
        Mode mode;
        int requested;
        int concurrency;
        int warmupDiscarded;
        double wallClockSeconds;
        Map<LatencySample.Outcome, Integer> outcomes;
        long[] total = new long[0];
        long[] dwell = new long[0];
        long[] handler = new long[0];
        long[] publishConfirm = new long[0];
        long[] dispatch = new long[0];
        long sequenceGaps;
        long outOfOrder;
    }

    /** Convenience for callers building their own series lists. */
    public static List<Long> boxed(long[] s) {
        List<Long> out = new ArrayList<>(s.length);
        for (long v : s) { out.add(v); }
        return out;
    }
}
