package com.solace.samples.requestreply.integration;

import com.solace.samples.requestreply.api.RequestReplyMessage;
import com.solace.samples.requestreply.api.RetryableHandlerException;
import com.solace.samples.requestreply.api.SolaceListener;
import com.solace.samples.requestreply.support.SolaceTestBroker;
import com.solace.samples.requestreply.transport.PersistentPublisher;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * A handler that throws {@link RetryableHandlerException} gets redelivered, not answered.
 *
 * <p>Proves both halves of the claim, not just the outcome: that redelivery is <em>active</em>
 * rather than the message merely sitting unacknowledged — settling FAILED is what makes the
 * broker try again without waiting for a disconnect — and that exhaustion still dead-letters
 * exactly like every other exhausted request, via the same {@code max-redelivery} path.
 *
 * <p>Replier-only ({@code reply.enabled=false}): this test needs no reply at all, since a
 * handler that always throws never produces one. Publishing goes straight through the shared
 * {@link PersistentPublisher} rather than {@code ReplyingSolaceTemplate}, which does not exist
 * in this context.
 */
@SpringBootTest(classes = RetryableHandlerExceptionIntegrationTest.App.class,
        webEnvironment = SpringBootTest.WebEnvironment.NONE,
        properties = {
                "solace.request-reply.reply.enabled=false",
                "solace.request-reply.replier.queue=" + RetryableHandlerExceptionIntegrationTest.QUEUE,
                "solace.request-reply.replier.topics=" + RetryableHandlerExceptionIntegrationTest.TOPIC,
                // Small on purpose: exhaustion has to be reachable inside the test's own wait.
                "solace.request-reply.replier.provision.max-redelivery=2",
                "solace.request-reply.dmq.enabled=true"
        })
class RetryableHandlerExceptionIntegrationTest {

    static final String QUEUE = "q.test.retry.requests";
    static final String TOPIC = "test/retry/v1/>";
    private static final String DMQ = "#DEAD_MSG_QUEUE";

    @DynamicPropertySource
    static void broker(DynamicPropertyRegistry registry) {
        registry.add("solace.java.host", SolaceTestBroker::smfHost);
        registry.add("solace.java.msg-vpn", SolaceTestBroker::vpn);
        registry.add("solace.java.client-username", SolaceTestBroker::username);
        registry.add("solace.java.client-password", SolaceTestBroker::password);
    }

    @Autowired PersistentPublisher publisher;
    @Autowired AlwaysThrowsHandler handler;

    @Test
    void isRedeliveredThenDeadLetteredRatherThanAnsweredOrLostForever() {
        handler.invocations.set(0);
        int before = SolaceTestBroker.queueDepth(DMQ);

        RequestReplyMessage message = RequestReplyMessage.of("retry me");
        message.setDmqEligible(true);
        publisher.publish(TOPIC.replace(">", "x"), message, null, 0);

        // Three deliveries total: the original attempt plus two redeliveries, one per
        // max-redelivery. If FAILED were not actively triggering redelivery, this would never
        // move past 1 -- the whole point of settling rather than merely not acknowledging.
        await().atMost(Duration.ofSeconds(20)).pollInterval(Duration.ofMillis(200))
                .untilAsserted(() -> assertThat(handler.invocations.get())
                        .as("each throw must provoke another delivery, not just one")
                        .isGreaterThanOrEqualTo(3));

        await().atMost(Duration.ofSeconds(20)).pollInterval(Duration.ofMillis(500))
                .untilAsserted(() -> assertThat(SolaceTestBroker.queueDepth(DMQ))
                        .as("exhausted retries must still dead-letter, the same as any other "
                                + "exhausted request")
                        .isGreaterThan(before));

        // No redelivery beyond exhaustion: a fixed delivery count, not a runaway loop.
        int atExhaustion = handler.invocations.get();
        try {
            Thread.sleep(1_500);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        assertThat(handler.invocations.get())
                .as("max-redelivery must actually bound the attempts")
                .isEqualTo(atExhaustion);
    }

    /**
     * {@code @EnableAutoConfiguration} without a component scan, so the only listener in this
     * context is the one declared below.
     */
    @Configuration
    @EnableAutoConfiguration
    static class App {

        @Bean
        AlwaysThrowsHandler alwaysThrowsHandler() { return new AlwaysThrowsHandler(); }
    }

    static class AlwaysThrowsHandler {

        final AtomicInteger invocations = new AtomicInteger();

        @SolaceListener(id = "retry-test", queue = QUEUE, topics = TOPIC,
                concurrency = "1", ackMode = "CLIENT")
        public void handle(@Payload byte[] body) {
            int attempt = invocations.incrementAndGet();
            throw new RetryableHandlerException("simulated transient failure, attempt " + attempt);
        }
    }
}
