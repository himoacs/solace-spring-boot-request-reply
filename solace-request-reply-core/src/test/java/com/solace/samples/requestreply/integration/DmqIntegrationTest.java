package com.solace.samples.requestreply.integration;

import com.solace.samples.requestreply.api.ReplyingSolaceTemplate;
import com.solace.samples.requestreply.config.SolaceRequestReplyProperties;
import com.solace.samples.requestreply.endpoint.DmqProvisioner;
import com.solace.samples.requestreply.support.SolaceTestBroker;
import com.solace.samples.requestreply.support.TestApp;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.time.Duration;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Dead-lettering: what happens to a request nobody ever handles.
 *
 * <p>Without this the failure is invisible. A request that outlives its TTL is deleted by the
 * broker, the requestor sees an ordinary timeout, and nothing anywhere records that a booking
 * was lost. The point of the feature is that the message survives somewhere inspectable, and the
 * point of this test is that it actually does.
 *
 * <p>Note the deliberate absence of a listener for {@code #TEST_TOPIC}: the request has to expire
 * on the queue, which cannot happen if anything consumes it. Using the demo's
 * {@code simulate=timeout} hook would prove nothing here, because that handler still
 * acknowledges the request — it declines to reply, which is a different thing entirely.
 */
@SpringBootTest(classes = TestApp.class,
        webEnvironment = SpringBootTest.WebEnvironment.NONE,
        properties = {
                "test.queue=q.test.dmq.requests",
                "test.subscription=test/dmq/handled/v1/>",
                "test.concurrency=1",

                "solace.request-reply.request.timeout=2s",
                "solace.request-reply.request.ttl-matches-timeout=true",
                "solace.request-reply.reply.topic-pattern=test/dmq/reply/v1/{instanceId}",
                "solace.request-reply.reply.queue-name-pattern=q.test.dmq.reply.{instanceId}",
                "solace.request-reply.replier.queue=q.test.dmq.requests",
                "solace.request-reply.replier.topics=test/dmq/handled/v1/>",
                // One redelivery attempt, so exhaustion is reachable quickly.
                "solace.request-reply.replier.provision.max-redelivery=1",

                "solace.request-reply.dmq.enabled=true",
                "solace.request-reply.dmq.name=" + DmqIntegrationTest.DMQ
        })
class DmqIntegrationTest {

    static final String DMQ = "#DEAD_MSG_QUEUE";
    /**
     * A queue with a subscription and no consumer at all. It has to be a separate queue: the
     * replier's own queue is drained by {@code TestApp}, so anything matching its subscription
     * gets handled rather than expiring, however unhandled the topic name looks.
     */
    private static final String ORPHAN_QUEUE = "q.test.dmq.orphan";
    private static final String UNHANDLED_TOPIC = "test/dmq/orphan/v1/never-consumed";

    @DynamicPropertySource
    static void broker(DynamicPropertyRegistry registry) {
        registry.add("solace.java.host", SolaceTestBroker::smfHost);
        registry.add("solace.java.msg-vpn", SolaceTestBroker::vpn);
        registry.add("solace.java.client-username", SolaceTestBroker::username);
        registry.add("solace.java.client-password", SolaceTestBroker::password);
    }

    @Autowired ReplyingSolaceTemplate template;
    @Autowired DmqProvisioner dmq;
    @Autowired SolaceRequestReplyProperties props;

    @Test
    void provisionsTheDmqAtStartup() {
        // Not lazily on first dead message: by then the broker has already deleted it.
        assertThat(dmq.isEstablished())
                .as("a DMQ that does not exist means the broker deletes instead of moving")
                .isTrue();
        assertThat(dmq.queueName()).isEqualTo(DMQ);
    }

    @Test
    void anExpiredRequestIsKeptRatherThanDiscarded() {
        assertThat(template.waitForReplyEndpoint(Duration.ofSeconds(20))).isTrue();
        SolaceTestBroker.createUnconsumedQueue(ORPHAN_QUEUE, UNHANDLED_TOPIC + "/>");
        int before = SolaceTestBroker.queueDepth(DMQ);

        // Nothing consumes this queue, so the request sits on it until its TTL expires.
        template.sendAndReceive(UNHANDLED_TOPIC + "/x", null, Map.of("value", "expires"),
                Map.class, Duration.ofSeconds(2));

        await().atMost(Duration.ofSeconds(45)).pollInterval(Duration.ofSeconds(1))
                .untilAsserted(() -> assertThat(SolaceTestBroker.queueDepth(DMQ))
                        .as("the expired request must survive in the DMQ, not vanish")
                        .isGreaterThan(before));

        assertThat(SolaceTestBroker.queueHasDmqEligibleMsg(DMQ))
                .as("published eligibility is what makes this work on brokers before 10.25.10")
                .isTrue();
    }

    @Test
    void replyTtlFollowsTheRequestTimeoutUnlessSet() {
        // The default has to track request.timeout rather than be a fixed duration: a hard-coded
        // reply TTL would expire replies while requestors were still waiting as soon as anyone
        // raised the timeout.
        assertThat(props.getReplier().getReplyTtl())
                .as("unset, so that it derives rather than pinning a value")
                .isNull();
        assertThat(props.getReplier().resolveReplyTtlMillis(props.getRequest().getTimeout()))
                .isEqualTo(2_000L);

        SolaceRequestReplyProperties.Replier explicit = new SolaceRequestReplyProperties.Replier();
        explicit.setReplyTtl(Duration.ofSeconds(30));
        assertThat(explicit.resolveReplyTtlMillis(Duration.ofSeconds(2))).isEqualTo(30_000L);

        explicit.setReplyTtl(Duration.ZERO);
        assertThat(explicit.resolveReplyTtlMillis(Duration.ofSeconds(2)))
                .as("an explicit zero disables expiry and must not fall back to the timeout")
                .isZero();
    }

    @Test
    void theCanaryIsNeverDeadLettered() {
        // The probe carries a TTL, so marking it eligible would deposit a canary in the DMQ on
        // every reconnect and bury the real failures this feature exists to surface.
        int before = SolaceTestBroker.queueDepth(DMQ);
        for (int i = 0; i < 3; i++) {
            assertThat(template.waitForReplyEndpoint(Duration.ofSeconds(10))).isTrue();
        }
        assertThat(SolaceTestBroker.queueDepth(DMQ)).isEqualTo(before);
    }
}
