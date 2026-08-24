package com.solace.samples.requestreply.integration;

import com.solace.samples.requestreply.api.ReplyingSolaceTemplate;
import com.solace.samples.requestreply.support.SolaceTestBroker;
import com.solace.samples.requestreply.support.TestApp;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Runs the minimal configuration published in the README, and nothing else.
 *
 * <p>A README example that does not work is worse than no example, and this one claims that
 * everything except these few properties has a usable default. That claim is easy to break by
 * adding a required property later, so it is asserted here rather than trusted.
 */
@SpringBootTest(classes = TestApp.class,
        webEnvironment = SpringBootTest.WebEnvironment.NONE,
        properties = {
                // Only what the TestApp listener needs in order to be wired at all.
                "test.queue=q.my.service.requests",
                "test.subscription=my/service/request/v1/>",
                "test.concurrency=1",

                // --- everything below is verbatim from the README's minimal example ---
                "solace.request-reply.request.timeout=5s",
                "solace.request-reply.reply.topic-pattern=my/service/reply/v1/{instanceId}",
                "solace.request-reply.reply.queue-name-pattern=q.my.service.reply.{instanceId}",
                "solace.request-reply.replier.queue=q.my.service.requests",
                "solace.request-reply.replier.topics=my/service/request/v1/>"
        })
class MinimalConfigIntegrationTest {

    @DynamicPropertySource
    static void broker(DynamicPropertyRegistry registry) {
        registry.add("solace.java.host", SolaceTestBroker::smfHost);
        registry.add("solace.java.msg-vpn", SolaceTestBroker::vpn);
        registry.add("solace.java.client-username", SolaceTestBroker::username);
        registry.add("solace.java.client-password", SolaceTestBroker::password);
    }

    @Autowired ReplyingSolaceTemplate template;

    @Test
    void theMinimalConfigurationRoundTrips() throws Exception {
        assertThat(template.waitForReplyEndpoint(Duration.ofSeconds(20))).isTrue();

        Map<?, ?> reply = template.sendAndReceive(
                        "my/service/request/v1/thing", null, Map.of("value", "minimal"),
                        Map.class, Duration.ofSeconds(10))
                .get(15, TimeUnit.SECONDS);

        assertThat(reply.get("echo")).isEqualTo("minimal");
    }
}
