package com.solace.samples.booking.web;

import com.solace.samples.requestreply.latency.LatencyRecorder;
import com.solace.samples.requestreply.latency.LatencyReport;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * Exposes the latency figures for whatever traffic has just been driven through the instance.
 *
 * <p>Same {@link LatencyReport} the standalone runner prints, rendered as JSON — one computation,
 * two presentations.
 */
@RestController
public class LoadTestController {

    private final LatencyRecorder.Collecting recorder;

    public LoadTestController(LatencyRecorder.Collecting recorder) {
        this.recorder = recorder;
    }

    /** Starts collecting samples. */
    @PostMapping("/api/latency/start")
    public Map<String, Object> start() {
        recorder.startCollecting();
        return Map.of("collecting", true);
    }

    /** Stops collecting and returns exact percentiles over what was captured. */
    @PostMapping("/api/latency/report")
    public Map<String, Object> report(@RequestParam(defaultValue = "0") int concurrency,
                                      @RequestParam(defaultValue = "0") double seconds) {
        var samples = recorder.stopCollecting();
        LatencyReport report = LatencyReport.of(samples, LatencyReport.Mode.CLOSED_LOOP,
                samples.size(), concurrency, 0, seconds);
        return report.toMap();
    }
}
