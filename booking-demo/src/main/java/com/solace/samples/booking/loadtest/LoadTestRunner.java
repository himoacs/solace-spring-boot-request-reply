package com.solace.samples.booking.loadtest;

import com.solace.samples.booking.domain.BookingRequest;
import com.solace.samples.booking.domain.SeatClass;
import com.solace.samples.booking.domain.SeatReservation;
import com.solace.samples.requestreply.api.ReplyingSolaceTemplate;
import com.solace.samples.requestreply.latency.HistogramRenderer;
import com.solace.samples.requestreply.latency.LatencyRecorder;
import com.solace.samples.requestreply.latency.LatencyReport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * Standalone latency test. Runs under the {@code loadtest} profile, prints a report, exits.
 *
 * <pre>
 * java -jar booking-demo.jar --spring.profiles.active=loadtest \
 *      --loadtest.count=100000 --loadtest.concurrency=64 --loadtest.warmup=2000
 * </pre>
 *
 * <p>Concurrency is bounded by a semaphore rather than fanned out unbounded. That is not
 * tidiness: persistent publishing is windowed, so past the transport window {@code send()}
 * blocks rather than buffering. Thousands of unbounded in-flight publishes stall on
 * back-pressure instead of going faster.
 */
@Component
@Profile("loadtest")
public class LoadTestRunner implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(LoadTestRunner.class);
    private static final String[] TRAINS = {"12951", "12621", "12301", "12009", "12002"};
    private static final String[] ZONES = {"nr", "sr", "wr", "er", "cr"};

    private final ApplicationContext context;
    private final ReplyingSolaceTemplate template;
    private final LatencyRecorder.Collecting recorder;
    private final String requestTopicPattern;
    private final int count;
    private final int concurrency;
    private final int warmup;
    private final LatencyReport.Mode mode;
    private final int ratePerSecond;

    public LoadTestRunner(ApplicationContext context,
                          ReplyingSolaceTemplate template,
                          LatencyRecorder.Collecting recorder,
                          @Value("${booking.topics.request-pattern}") String requestTopicPattern,
                          @Value("${loadtest.count:5000}") int count,
                          @Value("${loadtest.concurrency:32}") int concurrency,
                          @Value("${loadtest.warmup:200}") int warmup,
                          @Value("${loadtest.mode:CLOSED_LOOP}") LatencyReport.Mode mode,
                          @Value("${loadtest.rate:1000}") int ratePerSecond) {
        this.context = context;
        this.template = template;
        this.recorder = recorder;
        this.requestTopicPattern = requestTopicPattern;
        this.count = count;
        this.concurrency = concurrency;
        this.warmup = warmup;
        this.mode = mode;
        this.ratePerSecond = ratePerSecond;
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        if (!template.waitForReplyEndpoint(Duration.ofSeconds(20))) {
            log.error("Reply endpoint never became ready; aborting");
            return;
        }

        // Warmup absorbs JIT, connection establishment, first subscription and first-touch spool
        // allocation, all of which land on the first few hundred requests and would otherwise
        // dominate the very tail the report is about.
        if (warmup > 0) {
            log.info("Warming up with {} requests (discarded)...", warmup);
            drive(warmup, Math.min(concurrency, 16), LatencyReport.Mode.CLOSED_LOOP);
        }

        log.info("Running {} requests at concurrency {} in {} mode...", count, concurrency, mode);
        recorder.startCollecting();
        long start = System.nanoTime();
        drive(count, concurrency, mode);
        double seconds = (System.nanoTime() - start) / 1_000_000_000.0;
        List<com.solace.samples.requestreply.latency.LatencySample> samples = recorder.stopCollecting();

        LatencyReport report = LatencyReport.of(samples, mode, count, concurrency, warmup, seconds);
        System.out.println(HistogramRenderer.render("Seat reservation latency", report));

        // A one-off test has to exit. Without this the web server keeps the JVM alive after the
        // report is printed, which turns a measurement into something you have to remember to kill.
        System.exit(SpringApplication.exit(context, () -> 0));
    }

    private void drive(int n, int inFlight, LatencyReport.Mode m) throws InterruptedException {
        Semaphore slots = new Semaphore(inFlight);
        CountDownLatch done = new CountDownLatch(n);
        long intervalNanos = m == LatencyReport.Mode.OPEN_LOOP && ratePerSecond > 0
                ? 1_000_000_000L / ratePerSecond : 0;
        long nextSend = System.nanoTime();

        for (int i = 0; i < n; i++) {
            if (intervalNanos > 0) {
                // Open loop: hold the schedule regardless of replies, so generator queueing
                // shows up in the measurement instead of quietly reducing the offered load.
                nextSend += intervalNanos;
                long waitNanos = nextSend - System.nanoTime();
                if (waitNanos > 0) { TimeUnit.NANOSECONDS.sleep(waitNanos); }
            } else {
                slots.acquire();
            }
            BookingRequest req = randomRequest();
            String topic = requestTopicPattern
                    .replace("{zone}", req.zone())
                    .replace("{trainNo}", req.trainNo());
            try {
                template.sendAndReceive(topic, req, SeatReservation.class,
                                template.defaultReplyTimeout())
                        .whenComplete((res, err) -> {
                            if (intervalNanos == 0) { slots.release(); }
                            done.countDown();
                        });
            } catch (RuntimeException ex) {
                if (intervalNanos == 0) { slots.release(); }
                done.countDown();
            }
        }
        // Generous: every outstanding request must either reply or reach its timeout.
        done.await(120, TimeUnit.SECONDS);
    }

    private BookingRequest randomRequest() {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        String train = TRAINS[rnd.nextInt(TRAINS.length)];
        String zone = ZONES[rnd.nextInt(ZONES.length)];
        SeatClass cls = SeatClass.values()[rnd.nextInt(SeatClass.values().length)];
        String date = "2026-1%d-%02d".formatted(rnd.nextInt(0, 3), rnd.nextInt(1, 29));
        return new BookingRequest(zone, train, date, cls, "loadtest-" + rnd.nextInt(100000), 1, null);
    }
}
