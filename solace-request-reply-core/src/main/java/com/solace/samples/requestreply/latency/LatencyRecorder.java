package com.solace.samples.requestreply.latency;

import java.util.List;

/**
 * Sink for latency samples.
 *
 * <p>An interface because the expected use is a one-off test run rather than continuous
 * monitoring: {@link Collecting} keeps every sample so percentiles are <em>exact</em>, which is
 * both simpler and more accurate than histogram estimation for a bounded run. Estimation
 * structures exist because continuous production streams cannot retain every sample; a load
 * test can.
 */
public interface LatencyRecorder {

    void record(LatencySample sample);

    /** Discards everything. The default, so measurement costs nothing when unused. */
    LatencyRecorder NOOP = sample -> { };

    /** Retains every sample for exact percentiles. */
    final class Collecting implements LatencyRecorder {

        private final java.util.concurrent.ConcurrentLinkedQueue<LatencySample> samples =
                new java.util.concurrent.ConcurrentLinkedQueue<>();
        private volatile boolean collecting;

        public void startCollecting() {
            samples.clear();
            collecting = true;
        }

        public List<LatencySample> stopCollecting() {
            collecting = false;
            return List.copyOf(samples);
        }

        public boolean isCollecting() { return collecting; }

        @Override
        public void record(LatencySample sample) {
            if (collecting) { samples.add(sample); }
        }
    }
}
