package com.solace.samples.requestreply.integration;

import com.solace.samples.requestreply.api.SolaceListener;
import com.solace.samples.requestreply.config.SolaceRequestReplyAutoConfiguration;
import com.solace.samples.requestreply.support.SolaceTestBroker;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.handler.annotation.SendTo;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@code @SendTo("#{...}")} reads exactly like Spring Kafka's SpEL-evaluated reply destination,
 * but this library evaluates no SpEL there -- only {@code ${...}} property placeholders. Proves
 * that mistake fails context startup with a message naming the bad value, rather than silently
 * treating {@code "#{someBean.replyTopic()}"} as a literal topic name and misrouting every reply
 * to it with no error at all.
 */
class SendToSpelRejectionIntegrationTest {

    @Test
    void aSpelSendToValueFailsStartupInsteadOfSilentlyMisroutingReplies() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(
                        com.solace.spring.boot.autoconfigure.SolaceJavaAutoConfiguration.class,
                        SolaceRequestReplyAutoConfiguration.class))
                .withUserConfiguration(App.class)
                .withPropertyValues(
                        "solace.java.host=" + SolaceTestBroker.smfHost(),
                        "solace.java.msg-vpn=" + SolaceTestBroker.vpn(),
                        "solace.java.client-username=" + SolaceTestBroker.username(),
                        "solace.java.client-password=" + SolaceTestBroker.password(),
                        "solace.request-reply.request.timeout=5s",
                        "solace.request-reply.replier.queue=q.test.spel.requests",
                        "solace.request-reply.replier.topics=test/spel/v1/>")
                .run(context -> assertThat(context).hasFailed()
                        .getFailure()
                        .as("must name the bad value and say SpEL is not supported, not just that "
                                + "startup failed for some unrelated reason")
                        .hasMessageContaining("#{someBean.replyTopic()}")
                        .hasMessageContaining("SpEL"));
    }

    @Configuration
    static class App {

        @Bean
        Handler handler() { return new Handler(); }
    }

    static class Handler {

        @SolaceListener(id = "spel-test", queue = "q.test.spel.requests",
                topics = "test/spel/v1/>", concurrency = "1", ackMode = "CLIENT")
        @SendTo("#{someBean.replyTopic()}")
        public String handle(@Payload String body) {
            return body;
        }
    }
}
