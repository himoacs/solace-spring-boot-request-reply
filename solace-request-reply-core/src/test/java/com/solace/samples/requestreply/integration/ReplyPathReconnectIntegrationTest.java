package com.solace.samples.requestreply.integration;

import com.solace.samples.requestreply.api.ReplyingSolaceTemplate;
import com.solace.samples.requestreply.endpoint.ReplyEndpoint;
import com.solace.samples.requestreply.support.SolaceTestBroker;
import com.solace.samples.requestreply.support.TestApp;
import com.solace.samples.requestreply.transport.SolaceSession;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * The test that protects the default configuration.
 *
 * <p>A temporary reply queue is destroyed once its linger window passes and is then recreated by
 * the broker <b>without its topic subscription</b>. In that state the session is up, the flow is
 * bound, the queue exists, nothing logs an error — and every request times out for ever. It is the
 * worst kind of failure: invisible, and permanent until someone restarts the process.
 *
 * <p>{@code recreate-on-reconnect} exists to prevent it, and this is the only test that exercises
 * that path, because provoking it needs the connection severed from <em>outside</em> the client.
 * {@code closeSession()} will not do: that is a clean shutdown, not a reconnect.
 */
@SpringBootTest(classes = TestApp.class,
        webEnvironment = SpringBootTest.WebEnvironment.NONE,
        properties = {
                "test.queue=q.test.reconnect",
                "test.subscription=test/reconnect/request/v1/>",
                "test.concurrency=2",
                "solace.java.client-name=reconnect-test-client",
                "solace.request-reply.reply.endpoint-type=TEMPORARY",
                "solace.request-reply.reply.topic-pattern=test/reconnect/reply/v1/{instanceId}",
                "solace.request-reply.reply.queue-name-pattern=q.test.reconnect.reply.{instanceId}",
                "solace.request-reply.reply.recreate-on-reconnect=true",
                "solace.request-reply.replier.queue=q.test.reconnect",
                "solace.request-reply.replier.topics=test/reconnect/request/v1/>",
                "solace.request-reply.replier.concurrency=2",
                "solace.request-reply.request.timeout=8s",
                // Reconnect fast: the point is recovery, not patience.
                "solace.java.reconnect-retries=20",
                "solace.java.reconnect-retry-wait-in-millis=1000"
        })
class ReplyPathReconnectIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(ReplyPathReconnectIntegrationTest.class);
    private static final String TOPIC = "test/reconnect/request/v1/nr/12951";

    @DynamicPropertySource
    static void broker(DynamicPropertyRegistry registry) {
        registry.add("solace.java.host", SolaceTestBroker::smfHost);
        registry.add("solace.java.msg-vpn", SolaceTestBroker::vpn);
        registry.add("solace.java.client-username", SolaceTestBroker::username);
        registry.add("solace.java.client-password", SolaceTestBroker::password);
    }

    @Autowired ReplyingSolaceTemplate template;
    @Autowired SolaceSession session;
    @Autowired ReplyEndpoint replyEndpoint;

    @Test
    void repliesStillArriveAfterTheConnectionIsSevered() throws Exception {
        assertThat(template.waitForReplyEndpoint(Duration.ofSeconds(25))).isTrue();

        // Baseline: prove the path works before breaking it, so a failure afterwards is
        // attributable to the reconnect rather than to setup.
        assertThat(roundTrip("before")).isEqualTo("before");
        long reconnectsBefore = session.reconnectCount();

        String clientName = SolaceTestBroker.findClientName("reconnect-test-client");
        assertThat(clientName)
                .as("the client must be findable by name for the disconnect to target it")
                .isNotNull();

        log.info("Severing the connection for client '{}'", clientName);
        int status = SolaceTestBroker.disconnectClient(clientName);
        assertThat(status).isBetween(200, 299);

        await("session reconnects")
                .atMost(Duration.ofSeconds(60))
                .pollInterval(Duration.ofMillis(500))
                .until(() -> session.reconnectCount() > reconnectsBefore && session.isConnected());

        await("reply endpoint re-established")
                .atMost(Duration.ofSeconds(60))
                .pollInterval(Duration.ofMillis(500))
                .until(replyEndpoint::isEstablished);

        // The assertion that matters. If the subscription had not been re-applied, the queue
        // would exist, the flow would be bound, and this would hang until the timeout — which is
        // exactly the silent failure the recovery path is there to prevent.
        await("reply path carries traffic again")
                .atMost(Duration.ofSeconds(60))
                .pollInterval(Duration.ofSeconds(2))
                .ignoreExceptions()
                .untilAsserted(() -> assertThat(roundTrip("after")).isEqualTo("after"));
    }

    private String roundTrip(String value) throws Exception {
        Map<?, ?> reply = template.sendAndReceive(TOPIC, "12951", Map.of("value", value),
                Map.class, Duration.ofSeconds(8)).get(12, TimeUnit.SECONDS);
        return String.valueOf(reply.get("echo"));
    }
}
