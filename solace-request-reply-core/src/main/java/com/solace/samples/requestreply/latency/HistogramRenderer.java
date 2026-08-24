package com.solace.samples.requestreply.latency;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Renders a run as a plain-text report.
 *
 * <p>Log-scale buckets, doubling each row. Latency distributions have long tails, and linear
 * buckets collapse everything interesting into the first column while spending most of the chart
 * on empty space.
 */
public final class HistogramRenderer {

    private static final int BAR_WIDTH = 34;
    /** Eighth-blocks give sub-character resolution, so small buckets stay visible. */
    private static final char[] EIGHTHS = {' ', '▏', '▎', '▍', '▌',
                                           '▋', '▊', '▉'};
    private static final char FULL = '█';

    private HistogramRenderer() { }

    public static String render(String title, LatencyReport r) {
        StringBuilder b = new StringBuilder();
        b.append('\n').append(title).append('\n');
        b.append(String.format("%,d requests · concurrency %d · %s",
                r.requested(), r.concurrency(), r.mode()));
        if (r.warmupDiscarded() > 0) {
            b.append(String.format(" · %,d warmup discarded", r.warmupDiscarded()));
        }
        b.append('\n');
        b.append(String.format("completed in %.1fs · %,.0f req/s%n%n",
                r.wallClockSeconds(), r.throughput()));

        b.append("OUTCOMES\n");
        int completed = Math.max(1, r.completed());
        r.outcomes().forEach((outcome, count) -> b.append(String.format("  %-18s %,10d  %6.2f%%%n",
                outcome.name().toLowerCase(), count, 100.0 * count / completed)));

        b.append('\n').append("TOTAL ROUND TRIP\n");
        appendPercentiles(b, r.total());

        b.append('\n').append("DISTRIBUTION\n");
        appendHistogram(b, r.total());

        b.append('\n').append(String.format("%-22s %10s %10s%n", "SEGMENTS", "p50", "p99"));
        appendSegment(b, "publish confirm", r.publishConfirm());
        appendSegment(b, "queue dwell", r.dwell());
        appendSegment(b, "handler", r.handler());
        appendSegment(b, "dispatch delay", r.dispatch());

        b.append('\n').append("ORDERING\n");
        b.append(String.format("  %-22s %,10d   %s%n", "sequence gaps", r.sequenceGaps(),
                r.sequenceGaps() == 0 ? "no message loss" : "MESSAGES LOST"));
        b.append(String.format("  %-22s %,10d   %s%n", "out-of-order", r.outOfOrder(),
                r.outOfOrder() == 0 ? "in order" : "expected on a flat non-exclusive queue"));

        if (r.mode() == LatencyReport.Mode.CLOSED_LOOP) {
            b.append('\n')
             .append("NOTE  Closed-loop: this is service time at concurrency ")
             .append(r.concurrency()).append(", not latency at an arrival rate.\n")
             .append("      A slowdown makes the generator issue fewer requests, so the tail is\n")
             .append("      under-sampled and flatters reality. Use --loadtest.mode=OPEN_LOOP for\n")
             .append("      latency against a fixed arrival rate.\n");
        }
        return b.toString();
    }

    private static void appendPercentiles(StringBuilder b, long[] sorted) {
        Map<String, Double> ps = new LinkedHashMap<>();
        ps.put("p50", 0.50);
        ps.put("p90", 0.90);
        ps.put("p95", 0.95);
        ps.put("p99", 0.99);
        ps.put("p99.9", 0.999);
        ps.forEach((label, p) -> b.append(String.format("  %8s %12s%n",
                label, ms(LatencyReport.percentile(sorted, p)))));
        b.append(String.format("  %8s %12s%n", "max", ms(sorted.length == 0 ? 0 : sorted[sorted.length - 1])));
    }

    private static void appendSegment(StringBuilder b, String name, long[] sorted) {
        b.append(String.format("  %-20s %10s %10s%n", name,
                ms(LatencyReport.percentile(sorted, 0.50)),
                ms(LatencyReport.percentile(sorted, 0.99))));
    }

    private static void appendHistogram(StringBuilder b, long[] sortedMicros) {
        if (sortedMicros.length == 0) {
            b.append("  (no successful samples)\n");
            return;
        }
        long maxMicros = sortedMicros[sortedMicros.length - 1];
        long lowMs = 1;
        // One row per doubling, from 1ms up to whatever covers the slowest sample.
        java.util.List<long[]> rows = new java.util.ArrayList<>();
        long peak = 0;
        for (long lo = 0; lo < Math.max(2_000, maxMicros + 1); ) {
            long hi = lowMs * 1_000;
            long count = countBetween(sortedMicros, lo, hi);
            rows.add(new long[]{lo, hi, count});
            peak = Math.max(peak, count);
            lo = hi;
            lowMs *= 2;
        }
        for (long[] row : rows) {
            if (row[2] == 0) { continue; }
            b.append(String.format("  %5d - %5d ms %,9d  %s%n",
                    row[0] / 1_000, row[1] / 1_000, row[2], bar(row[2], peak)));
        }
    }

    private static long countBetween(long[] sorted, long loInclusive, long hiExclusive) {
        int lo = lowerBound(sorted, loInclusive);
        int hi = lowerBound(sorted, hiExclusive);
        return hi - lo;
    }

    private static int lowerBound(long[] sorted, long value) {
        int lo = 0;
        int hi = sorted.length;
        while (lo < hi) {
            int mid = (lo + hi) >>> 1;
            if (sorted[mid] < value) { lo = mid + 1; } else { hi = mid; }
        }
        return lo;
    }

    private static String bar(long count, long peak) {
        if (peak <= 0 || count <= 0) { return ""; }
        double filled = (double) count / peak * BAR_WIDTH;
        int full = (int) filled;
        int remainder = (int) Math.round((filled - full) * 8);
        StringBuilder s = new StringBuilder();
        s.append(String.valueOf(FULL).repeat(Math.max(0, full)));
        if (remainder > 0 && remainder < EIGHTHS.length) { s.append(EIGHTHS[remainder]); }
        return s.isEmpty() ? String.valueOf(EIGHTHS[1]) : s.toString();
    }

    private static String ms(long micros) {
        return String.format("%.1f ms", micros / 1000.0);
    }
}
