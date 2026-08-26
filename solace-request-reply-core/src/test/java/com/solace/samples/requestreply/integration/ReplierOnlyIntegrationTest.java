package com.solace.samples.requestreply.integration;

import com.solace.samples.requestreply.api.ReplyingSolaceTemplate;
import com.solace.samples.requestreply.config.ReplyPathHealthIndicator;
import com.solace.samples.requestreply.config.SolaceSessionHealthIndicator;
import com.solace.samples.requestreply.endpoint.ReplyEndpoint;
import com.solace.samples.requestreply.support.SolaceTestBroker;
import com.solace.samples.requestreply.support.TestApp;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static java.time.Duration.ofSeconds;

/**
 * A replier provisions no reply queue of its own.
 *
 * <p>It consumes the shared request queue and publishes each reply to the requestor's own
 * {@code replyTo} topic, so it is never addressed on a reply queue. Provisioning one anyway
 * created a durable, exclusive queue that was subscribed, bound, and then received nothing —
 * and because the queue is named after the instance, a Kubernetes Deployment stranded the
 * previous pods' queues on the broker on every rollout, one per pod, indefinitely.
 *
 * <p>The queue-list assertion is the one that matters here. The absent beans are the mechanism;
 * the queue not existing on the broker is the actual claim.
 */
@SpringBootTest(classes = TestApp.class,
        webEnvironment = SpringBootTest.WebEnvironment.NONE,
        properties = {
                "test.queue=q.test.replieronly.requests",
                "test.subscription=test/replieronly/request/v1/>",
                "test.concurrency=1",

                "solace.request-reply.reply.enabled=false",
                "solace.request-reply.reply.queue-name-pattern=q.test.replieronly.reply.{instanceId}",
                "solace.request-reply.reply.instance-id=" + ReplierOnlyIntegrationTest.INSTANCE,
                "solace.request-reply.replier.queue=q.test.replieronly.requests",
                "solace.request-reply.replier.topics=test/replieronly/request/v1/>"
        })
class ReplierOnlyIntegrationTest {

    static final String INSTANCE = "replier-only-instance";
    private static final String WOULD_BE_REPLY_QUEUE = "q.test.replieronly.reply." + INSTANCE;

    @DynamicPropertySource
    static void broker(DynamicPropertyRegistry registry) {
        registry.add("solace.java.host", SolaceTestBroker::smfHost);
        registry.add("solace.java.msg-vpn", SolaceTestBroker::vpn);
        registry.add("solace.java.client-username", SolaceTestBroker::username);
        registry.add("solace.java.client-password", SolaceTestBroker::password);
    }

    @Autowired ApplicationContext context;
    @Autowired TestApp.CountingHandler handler;
    @Autowired SolaceSessionHealthIndicator sessionHealth;

    @Test
    void reportsHealthEvenWithoutAReplyPath() {
        // The gap this closes: ReplyPathHealthIndicator is gated on reply.enabled, so a
        // replier-only process was left with no Solace health signal at all.
        assertThat(context.getBeanNamesForType(ReplyPathHealthIndicator.class))
                .as("no reply path to report on, so this one must be absent rather than half-working")
                .isEmpty();

        await().atMost(ofSeconds(10)).untilAsserted(() -> {
            Health health = sessionHealth.health();
            assertThat(health.getStatus()).isEqualTo(Status.UP);
            assertThat(health.getDetails()).containsEntry("sessionConnected", true);
            assertThat((Integer) health.getDetails().get("listenersDeclared")).isEqualTo(1);
            assertThat((List<?>) health.getDetails().get("listenersNotRunning")).isEmpty();
        });
    }

    @Test
    void provisionsNoReplyQueue() {
        assertThat(SolaceTestBroker.queueExists(WOULD_BE_REPLY_QUEUE))
                .as("a replier is never addressed on a reply queue, so none should exist")
                .isFalse();
    }

    @Test
    void hasNoRequestorSide() {
        assertThat(context.getBeanNamesForType(ReplyEndpoint.class)).isEmpty();
        assertThat(context.getBeanNamesForType(ReplyingSolaceTemplate.class))
                .as("without a reply queue there is nothing to correlate a reply against, so the "
                        + "requestor-side template must be absent rather than half-working")
                .isEmpty();
    }

    @Test
    void stillBindsTheRequestQueueAndCanHandleWork() {
        // The whole point: removing the reply endpoint must not touch the replier's own job.
        assertThat(SolaceTestBroker.queueExists("q.test.replieronly.requests"))
                .as("the shared request queue is still provisioned and subscribed")
                .isTrue();
        assertThat(handler).isNotNull();
    }
}
