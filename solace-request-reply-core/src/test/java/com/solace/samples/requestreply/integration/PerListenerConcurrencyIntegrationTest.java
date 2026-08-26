package com.solace.samples.requestreply.integration;

import com.solace.samples.requestreply.api.ReplyingSolaceTemplate;
import com.solace.samples.requestreply.api.SolaceListener;
import com.solace.samples.requestreply.support.SolaceTestBroker;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A slow handler on one {@code @SolaceListener} must not stall a fast handler on another.
 *
 * <p>Measured against a live broker before this test existed: JCSMP dispatches every flow in a
 * session on <b>one shared thread</b> — four flows delivering a 400&nbsp;ms handler took ~4.8s,
 * not the ~1.2s four-way parallelism would give. So the concurrency a listener actually gets
 * comes entirely from the handler pool it is handed, not from its flow count.
 *
 * <p>Before this test's fix existed, that pool was a single process-wide bean sized from
 * {@code replier.concurrency} — a property, not the annotation attribute — so every
 * {@code @SolaceListener} in the process shared it regardless of what each one declared. Two
 * listeners each asking for {@code concurrency = "1"} were really asking for a slice of the
 * <em>same</em> one thread. This context reproduces exactly that configuration —
 * {@code replier.concurrency=1} alongside two independent listeners — so it fails against a
 * shared pool and passes only when each container's pool truly belongs to that container alone.
 */
@SpringBootTest(classes = PerListenerConcurrencyIntegrationTest.App.class,
        webEnvironment = SpringBootTest.WebEnvironment.NONE,
        properties = {
                "solace.request-reply.request.timeout=6s",
                "solace.request-reply.reply.topic-pattern=test/plc/reply/v1/{instanceId}",
                "solace.request-reply.reply.queue-name-pattern=q.test.plc.reply.{instanceId}",
                // Not read by either container any more (see the class javadoc above) — set to 1
                // so this test would fail immediately against the pre-fix shared-pool code.
                "solace.request-reply.replier.concurrency=1"
        })
class PerListenerConcurrencyIntegrationTest {

    static final String SLOW_QUEUE = "q.test.plc.slow";
    static final String SLOW_TOPIC = "test/plc/slow/v1/>";
    static final String FAST_QUEUE = "q.test.plc.fast";
    static final String FAST_TOPIC = "test/plc/fast/v1/>";
    static final Duration SLOW_HANDLER_DURATION = Duration.ofSeconds(3);

    @DynamicPropertySource
    static void broker(DynamicPropertyRegistry registry) {
        registry.add("solace.java.host", SolaceTestBroker::smfHost);
        registry.add("solace.java.msg-vpn", SolaceTestBroker::vpn);
        registry.add("solace.java.client-username", SolaceTestBroker::username);
        registry.add("solace.java.client-password", SolaceTestBroker::password);
    }

    @Autowired ReplyingSolaceTemplate template;

    @Test
    void aSlowHandlerOnOneListenerDoesNotStallAFastHandlerOnAnother() throws Exception {
        assertThat(template.waitForReplyEndpoint(Duration.ofSeconds(20))).isTrue();

        // Occupies the slow listener's one-thread pool for SLOW_HANDLER_DURATION. Not awaited
        // yet — its future only matters for the assertion after the fast one has already proven
        // the point.
        var slow = template.sendAndReceive(SLOW_TOPIC + "/x", Map.of("v", "slow"),
                Map.class, Duration.ofSeconds(6));

        // Give the slow request a head start onto its listener's single thread before the fast
        // one is sent, so an accidental shared pool would have no free thread to grant it.
        Thread.sleep(400);

        long start = System.nanoTime();
        Map<?, ?> fastReply = template.sendAndReceive(FAST_TOPIC + "/x", Map.of("v", "fast"),
                        Map.class, Duration.ofSeconds(2))
                .get(2, TimeUnit.SECONDS);
        long fastElapsedMs = (System.nanoTime() - start) / 1_000_000;

        assertThat(fastReply.get("echo")).isEqualTo("fast");
        // Comfortably under SLOW_HANDLER_DURATION: if the two listeners shared one pool, the
        // fast reply would only arrive after the slow handler released its thread, ~3s later.
        assertThat(fastElapsedMs).isLessThan(1_500);

        assertThat(slow.get(6, TimeUnit.SECONDS).get("echo")).isEqualTo("slow");
    }

    /**
     * {@code @EnableAutoConfiguration} without a component scan, so the only listeners in this
     * context are the two declared below.
     */
    @Configuration
    @EnableAutoConfiguration
    static class App {

        @Bean
        SlowHandler slowHandler() { return new SlowHandler(); }

        @Bean
        FastHandler fastHandler() { return new FastHandler(); }
    }

    static class SlowHandler {

        @SolaceListener(id = "plc-slow", queue = SLOW_QUEUE, topics = SLOW_TOPIC,
                concurrency = "1", ackMode = "CLIENT")
        @SendTo
        public Map<String, Object> handle(@Payload Map<String, Object> body) throws InterruptedException {
            Thread.sleep(SLOW_HANDLER_DURATION.toMillis());
            return Map.of("echo", body.getOrDefault("v", ""));
        }
    }

    static class FastHandler {

        @SolaceListener(id = "plc-fast", queue = FAST_QUEUE, topics = FAST_TOPIC,
                concurrency = "1", ackMode = "CLIENT")
        @SendTo
        public Map<String, Object> handle(@Payload Map<String, Object> body) {
            return Map.of("echo", body.getOrDefault("v", ""));
        }
    }
}
