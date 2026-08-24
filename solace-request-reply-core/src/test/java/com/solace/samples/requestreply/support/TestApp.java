package com.solace.samples.requestreply.support;

import com.solace.samples.requestreply.api.SolaceHeaders;
import com.solace.samples.requestreply.api.SolaceListener;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/** Minimal application used by the integration tests. */
@SpringBootApplication
public class TestApp {

    /**
     * Records how many times work was actually performed, keyed by correlation id.
     *
     * <p>{@link #invocations} counts every delivery, while {@link #distinctWork} counts unique
     * correlation ids. The gap between them is the whole point: with competing consumers and
     * at-least-once delivery, deliveries can exceed work — but work must never exceed one per
     * correlation id, or a booking has been made twice.
     */
    @Component
    public static class CountingHandler {

        private final AtomicInteger invocations = new AtomicInteger();
        private final Map<String, String> resultsByCorrelationId = new ConcurrentHashMap<>();

        @SolaceListener(id = "test-echo",
                queue = "${test.queue}",
                topics = "${test.subscription}",
                concurrency = "${test.concurrency:3}",
                ackMode = "CLIENT")
        @SendTo
        public Map<String, Object> handle(@Payload Map<String, Object> body,
                                          @Header(SolaceHeaders.CORRELATION_ID) String correlationId) {
            invocations.incrementAndGet();
            String value = resultsByCorrelationId.computeIfAbsent(correlationId,
                    id -> "result-" + java.util.UUID.randomUUID());
            return Map.of("echo", body.getOrDefault("value", ""),
                    "result", value,
                    "correlationId", correlationId);
        }

        public int invocations() { return invocations.get(); }

        public int distinctWork() { return resultsByCorrelationId.size(); }

        public String resultFor(String correlationId) { return resultsByCorrelationId.get(correlationId); }

        public void reset() {
            invocations.set(0);
            resultsByCorrelationId.clear();
        }
    }
}
