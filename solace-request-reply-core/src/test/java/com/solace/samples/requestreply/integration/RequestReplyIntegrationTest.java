package com.solace.samples.requestreply.integration;

import com.solace.samples.requestreply.api.ReplyingSolaceTemplate;
import com.solace.samples.requestreply.api.RequestReplyFuture;
import com.solace.samples.requestreply.core.CorrelationStore;
import com.solace.samples.requestreply.exception.RequestTimeoutException;
import com.solace.samples.requestreply.support.SolaceTestBroker;
import com.solace.samples.requestreply.support.TestApp;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * The correctness properties that guaranteed messaging depends on.
 *
 * <p>Note {@code test.concurrency=3}: three flows compete over one queue, which is the
 * configuration that would fan out and triple-book if the replier used a direct topic
 * subscription instead of a queue.
 */
@SpringBootTest(classes = TestApp.class,
        webEnvironment = SpringBootTest.WebEnvironment.NONE,
        properties = {
                "test.queue=q.test.requestreply",
                "test.subscription=test/rr/request/v1/>",
                "test.concurrency=3",
                "solace.request-reply.reply.topic-pattern=test/rr/reply/v1/{instanceId}",
                "solace.request-reply.reply.queue-name-pattern=q.test.reply.{instanceId}",
                "solace.request-reply.replier.queue=q.test.requestreply",
                "solace.request-reply.replier.topics=test/rr/request/v1/>",
                "solace.request-reply.replier.concurrency=3",
                "solace.request-reply.request.timeout=6s"
        })
class RequestReplyIntegrationTest {

    private static final String TOPIC = "test/rr/request/v1/nr/12951";

    @DynamicPropertySource
    static void broker(DynamicPropertyRegistry registry) {
        registry.add("solace.java.host", SolaceTestBroker::smfHost);
        registry.add("solace.java.msg-vpn", SolaceTestBroker::vpn);
        registry.add("solace.java.client-username", SolaceTestBroker::username);
        registry.add("solace.java.client-password", SolaceTestBroker::password);
    }

    @Autowired ReplyingSolaceTemplate template;
    @Autowired TestApp.CountingHandler handler;
    @Autowired CorrelationStore store;

    @BeforeEach
    void reset() {
        handler.reset();
        assertThat(template.waitForReplyEndpoint(Duration.ofSeconds(20))).isTrue();
    }

    @Test
    void roundTripsAndConfirmsThePublishSeparately() throws Exception {
        RequestReplyFuture<Map> future = template.sendAndReceive(
                TOPIC, Map.of("value", "hello"), Map.class, Duration.ofSeconds(6));

        // The send future must resolve on its own, before and independently of the reply: that
        // separation is what distinguishes "never landed" from "nobody answered".
        var publish = future.getSendFuture().get(10, TimeUnit.SECONDS);
        assertThat(publish.confirmNanos()).isPositive();

        Map<?, ?> reply = future.get(10, TimeUnit.SECONDS);
        assertThat(reply.get("echo")).isEqualTo("hello");
    }

    @Test
    void oneRequestDoesWorkExactlyOnceDespiteCompetingConsumers() throws Exception {
        template.sendAndReceive(TOPIC, Map.of("value", "single"), Map.class,
                Duration.ofSeconds(6)).get(10, TimeUnit.SECONDS);

        // Three flows are bound. A direct topic subscription would have delivered to all three,
        // executing the handler three times for one request — on a booking system, three seats.
        assertThat(handler.distinctWork())
                .as("one request must produce exactly one unit of work")
                .isEqualTo(1);
        assertThat(handler.invocations())
                .as("and it must be delivered once, not once per bound flow")
                .isEqualTo(1);
    }

    @Test
    void replayingACorrelationIdDoesNotRepeatTheWork() throws Exception {
        String correlationId = "replay-" + UUID.randomUUID();

        Map<?, ?> first = template.sendAndReceive(TOPIC, Map.of("value", "once"),
                Map.class, Duration.ofSeconds(6), correlationId).get(10, TimeUnit.SECONDS);
        Map<?, ?> second = template.sendAndReceive(TOPIC, Map.of("value", "once"),
                Map.class, Duration.ofSeconds(6), correlationId).get(10, TimeUnit.SECONDS);

        assertThat(handler.distinctWork())
                .as("a redelivered correlation id must not book a second seat")
                .isEqualTo(1);
        assertThat(second.get("result"))
                .as("the replay must return the original result, not a new one")
                .isEqualTo(first.get("result"));
    }

    @Test
    void concurrentRequestsAreCorrelatedIndependently() throws Exception {
        int n = 60;
        var futures = new java.util.ArrayList<RequestReplyFuture<Map>>();
        for (int i = 0; i < n; i++) {
            futures.add(template.sendAndReceive(TOPIC, Map.of("value", "req-" + i),
                    Map.class, Duration.ofSeconds(15)));
        }
        for (int i = 0; i < n; i++) {
            assertThat(futures.get(i).get(20, TimeUnit.SECONDS).get("echo")).isEqualTo("req-" + i);
        }
        assertThat(handler.distinctWork()).isEqualTo(n);
        await().atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> assertThat(store.size())
                        .as("no pending entry may be left behind")
                        .isZero());
    }

    @Test
    void anUnansweredRequestTimesOutAndIsEvicted() {
        // Nothing subscribes here, so the request is spooled to no queue and no replier sees it.
        RequestReplyFuture<Map> future = template.sendAndReceive(
                "test/rr/unrouted/v1/nobody", Map.of("value", "x"), Map.class,
                Duration.ofMillis(700));

        assertThatThrownBy(() -> future.get(10, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(RequestTimeoutException.class);

        await().atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> assertThat(store.size())
                        .as("the reaper must evict it rather than leaking the future")
                        .isZero());
    }
}
