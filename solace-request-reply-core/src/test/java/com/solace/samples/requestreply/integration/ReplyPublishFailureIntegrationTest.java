package com.solace.samples.requestreply.integration;

import com.solace.samples.requestreply.api.RequestReplyMessage;
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
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * A reply that cannot be published settles the request FAILED, rather than leaving it
 * unacknowledged and inert.
 *
 * <p>Before this, both the synchronous-throw path (publishing raised before a ticket even
 * existed) and the async-rejection path (the broker rejected an already-sent publish) just
 * logged "leaving the request unacknowledged so it is redelivered" — which was false while the
 * connection stayed up; an unacked CLIENT-ack message only redelivers on disconnect.
 *
 * <p>Forces the synchronous path deterministically and without any broker permission setup: an
 * explicit {@code @SendTo} destination one character over Solace's 250-character topic limit
 * makes JCSMP's own {@code createTopic()} throw {@code IllegalArgumentException} the moment the
 * container tries to publish the reply — exactly the {@code catch (RuntimeException ex)} branch
 * around the reply publish in {@code SolaceMessageListenerContainer.handle()}. The destination
 * is fixed on the listener rather than carried on the request, so only the reply's own publish
 * fails; the request this test publishes directly is untouched by it.
 */
@SpringBootTest(classes = ReplyPublishFailureIntegrationTest.App.class,
        webEnvironment = SpringBootTest.WebEnvironment.NONE,
        properties = {
                "solace.request-reply.reply.enabled=false",
                "solace.request-reply.replier.queue=" + ReplyPublishFailureIntegrationTest.QUEUE,
                "solace.request-reply.replier.topics=" + ReplyPublishFailureIntegrationTest.TOPIC,
                // Small on purpose: exhaustion has to be reachable inside the test's own wait.
                "solace.request-reply.replier.provision.max-redelivery=2",
                "solace.request-reply.dmq.enabled=true"
        })
class ReplyPublishFailureIntegrationTest {

    static final String QUEUE = "q.test.replyfail.requests";
    static final String TOPIC = "test/replyfail/v1/>";
    private static final String DMQ = "#DEAD_MSG_QUEUE";
    /** One character over Solace's 250-character topic limit. */
    static final String OVERSIZED_REPLY_TOPIC = "test/replyfail/reply/v1/" + "x".repeat(230);

    @DynamicPropertySource
    static void broker(DynamicPropertyRegistry registry) {
        registry.add("solace.java.host", SolaceTestBroker::smfHost);
        registry.add("solace.java.msg-vpn", SolaceTestBroker::vpn);
        registry.add("solace.java.client-username", SolaceTestBroker::username);
        registry.add("solace.java.client-password", SolaceTestBroker::password);
        // Built with String.repeat(), which disqualifies it from being an annotation
        // constant -- hence a dynamic property rather than a literal in properties = {...}.
        registry.add("test.oversized-reply-topic", () -> OVERSIZED_REPLY_TOPIC);
    }

    @Autowired PersistentPublisher publisher;
    @Autowired EchoHandler handler;

    @Test
    void anUnpublishableReplySettlesTheRequestFailedInsteadOfStalling() {
        handler.invocations.set(0);
        int before = SolaceTestBroker.queueDepth(DMQ);

        // An ordinary request. Nothing about it is malformed -- the oversized destination is
        // fixed on the listener's own @SendTo, not carried here, so this publish itself must
        // succeed cleanly; only the reply the handler tries to send back can fail.
        RequestReplyMessage message = RequestReplyMessage.of("hello");
        message.setCorrelationId("replyfail-1");
        message.setDmqEligible(true);
        publisher.publish(TOPIC.replace(">", "x"), message, null, 0);

        // Three deliveries total: the original attempt plus two redeliveries, one per
        // max-redelivery. If the request were merely left unacknowledged instead of settled
        // FAILED, this would never move past 1.
        await().atMost(Duration.ofSeconds(20)).pollInterval(Duration.ofMillis(200))
                .untilAsserted(() -> assertThat(handler.invocations.get())
                        .as("each unpublishable reply must provoke another delivery of the request")
                        .isGreaterThanOrEqualTo(3));

        await().atMost(Duration.ofSeconds(20)).pollInterval(Duration.ofMillis(500))
                .untilAsserted(() -> assertThat(SolaceTestBroker.queueDepth(DMQ))
                        .as("exhausted retries must still dead-letter the request")
                        .isGreaterThan(before));
    }

    /**
     * {@code @EnableAutoConfiguration} without a component scan, so the only listener in this
     * context is the one declared below.
     */
    @Configuration
    @EnableAutoConfiguration
    static class App {

        @Bean
        EchoHandler echoHandler() { return new EchoHandler(); }
    }

    static class EchoHandler {

        final AtomicInteger invocations = new AtomicInteger();

        @SolaceListener(id = "replyfail-test", queue = QUEUE, topics = TOPIC,
                concurrency = "1", ackMode = "CLIENT")
        @SendTo("${test.oversized-reply-topic}")
        public Map<String, Object> handle(@Payload byte[] body) {
            invocations.incrementAndGet();
            return Map.of("ok", true);
        }
    }
}
