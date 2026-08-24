package com.solace.samples.requestreply.integration;

import com.solace.samples.requestreply.api.ReplyingSolaceTemplate;
import com.solace.samples.requestreply.config.ReplyPathHealthIndicator;
import com.solace.samples.requestreply.endpoint.ReplyEndpoint;
import com.solace.samples.requestreply.core.DefaultReplyingSolaceTemplate;
import com.solace.samples.requestreply.support.SolaceTestBroker;
import com.solace.samples.requestreply.support.TestApp;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The probe that turns a broken reply path into something observable.
 *
 * <p>Re-establishing an endpoint can appear to succeed while replies still cannot arrive, which is
 * exactly what happens to a temporary queue recreated without its subscription. The probe is the
 * only check that distinguishes the two, because it requires a message to complete the round trip.
 */
@SpringBootTest(classes = TestApp.class,
        webEnvironment = SpringBootTest.WebEnvironment.NONE,
        properties = {
                "test.queue=q.test.canary",
                "test.subscription=test/canary/request/v1/>",
                "test.concurrency=1",
                "solace.request-reply.reply.endpoint-type=DURABLE",
                "solace.request-reply.reply.topic-pattern=test/canary/reply/v1/{instanceId}",
                "solace.request-reply.reply.queue-name-pattern=q.test.canary.reply.{instanceId}",
                "solace.request-reply.reply.canary-on-reconnect=true",
                "solace.request-reply.reply.canary-timeout=8s",
                "solace.request-reply.replier.queue=q.test.canary",
                "solace.request-reply.replier.topics=test/canary/request/v1/>",
                "solace.request-reply.replier.concurrency=1"
        })
class ReplyPathCanaryIntegrationTest {

    @DynamicPropertySource
    static void broker(DynamicPropertyRegistry registry) {
        registry.add("solace.java.host", SolaceTestBroker::smfHost);
        registry.add("solace.java.msg-vpn", SolaceTestBroker::vpn);
        registry.add("solace.java.client-username", SolaceTestBroker::username);
        registry.add("solace.java.client-password", SolaceTestBroker::password);
    }

    @Autowired ReplyingSolaceTemplate template;
    @Autowired ReplyPathHealthIndicator replyPathHealth;
    @Autowired ReplyEndpoint replyEndpoint;

    @Test
    void theProbeCompletesAndHealthReportsUp() {
        DefaultReplyingSolaceTemplate impl = (DefaultReplyingSolaceTemplate) template;

        assertThat(impl.verifyReplyPath())
                .as("a probe published to our own reply topic must come back")
                .isTrue();
        assertThat(impl.isReplyPathVerified()).isTrue();

        Health health = replyPathHealth.health();
        assertThat(health.getStatus()).isEqualTo(Status.UP);
        assertThat(health.getDetails())
                .containsEntry("replyPathVerified", true)
                .containsEntry("sessionConnected", true)
                .containsKey("replySubscription");
    }

    @Test
    void healthReportsDownWhenTheProbeCannotReturn() {
        DefaultReplyingSolaceTemplate impl = (DefaultReplyingSolaceTemplate) template;
        assertThat(impl.verifyReplyPath()).isTrue();

        // Remove the subscription behind the library's back. This reproduces the state a temporary
        // reply queue is left in once its linger window expires: the queue exists and the flow
        // stays bound, so nothing else notices, but no reply can be delivered.
        String queue = replyEndpoint.queue().getName();
        var res = SolaceTestBroker.semp("DELETE",
                "/SEMP/v2/config/msgVpns/" + SolaceTestBroker.vpn()
                        + "/queues/" + enc(queue)
                        + "/subscriptions/" + enc(replyEndpoint.subscription()),
                null);
        assertThat(res.statusCode())
                .as("the subscription must actually have been removed for this test to mean anything")
                .isBetween(200, 299);

        assertThat(impl.verifyReplyPath())
                .as("with no subscription the probe cannot return, and that must be detected")
                .isFalse();
        assertThat(replyPathHealth.health().getStatus())
                .as("so the instance reports itself unfit to serve requests")
                .isEqualTo(Status.DOWN);

        // Recover through the library's own path rather than out of band. This both leaves the
        // shared context usable for other tests and shows that re-applying the subscription is
        // all the recovery that is needed.
        replyEndpoint.applySubscription();
        assertThat(impl.verifyReplyPath())
                .as("re-applying the subscription must restore the reply path")
                .isTrue();
        assertThat(replyPathHealth.health().getStatus()).isEqualTo(Status.UP);
    }

    private static String enc(String s) {
        return java.net.URLEncoder.encode(s, java.nio.charset.StandardCharsets.UTF_8);
    }
}
