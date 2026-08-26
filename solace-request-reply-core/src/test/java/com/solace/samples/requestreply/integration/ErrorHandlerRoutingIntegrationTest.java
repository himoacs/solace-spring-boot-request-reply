package com.solace.samples.requestreply.integration;

import com.solace.samples.requestreply.api.ReplyingSolaceTemplate;
import com.solace.samples.requestreply.api.SolaceListener;
import com.solace.samples.requestreply.api.SolaceListenerErrorHandler;
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
 * {@code @SolaceListener(errorHandler = "...")} selects a handler by bean name.
 *
 * <p>Two things are asserted at once, and both used to fail. The attribute was parsed and never
 * read, so every listener silently got whichever single handler bean existed; and the lookup was
 * {@code getIfAvailable()}, which throws {@code NoUniqueBeanDefinitionException} during context
 * refresh as soon as a second handler bean appears — an error naming neither the listener nor the
 * attribute that was supposed to disambiguate it.
 *
 * <p>So this context deliberately defines <b>two</b> handlers. Starting at all proves the
 * ambiguity no longer breaks refresh; the reply naming {@code chosen} proves the attribute is
 * actually honoured rather than coincidentally right.
 */
@SpringBootTest(classes = ErrorHandlerRoutingIntegrationTest.App.class,
        webEnvironment = SpringBootTest.WebEnvironment.NONE,
        properties = {
                "solace.request-reply.request.timeout=5s",
                "solace.request-reply.reply.topic-pattern=test/errh/reply/v1/{instanceId}",
                "solace.request-reply.reply.queue-name-pattern=q.test.errh.reply.{instanceId}",
                "solace.request-reply.replier.queue=" + ErrorHandlerRoutingIntegrationTest.QUEUE,
                "solace.request-reply.replier.topics=" + ErrorHandlerRoutingIntegrationTest.TOPICS
        })
class ErrorHandlerRoutingIntegrationTest {

    static final String QUEUE = "q.test.errh.requests";
    static final String TOPICS = "test/errh/request/v1/>";

    @DynamicPropertySource
    static void broker(DynamicPropertyRegistry registry) {
        registry.add("solace.java.host", SolaceTestBroker::smfHost);
        registry.add("solace.java.msg-vpn", SolaceTestBroker::vpn);
        registry.add("solace.java.client-username", SolaceTestBroker::username);
        registry.add("solace.java.client-password", SolaceTestBroker::password);
    }

    @Autowired ReplyingSolaceTemplate template;

    @Test
    void theNamedErrorHandlerAnswersRatherThanTheOtherOne() throws Exception {
        assertThat(template.waitForReplyEndpoint(Duration.ofSeconds(20))).isTrue();

        Map<?, ?> reply = template.sendAndReceive("test/errh/request/v1/thing",
                        Map.of("value", "boom"), Map.class, Duration.ofSeconds(10))
                .get(15, TimeUnit.SECONDS);

        // Not "notThisOne", and not a RemoteErrorException: the named handler converted the
        // failure into an ordinary reply, which is the contract the attribute advertises.
        assertThat(reply.get("handledBy")).isEqualTo("chosen");
    }

    /**
     * {@code @EnableAutoConfiguration} without a component scan, so this test's listener is the
     * only one in the context — a scan would also pick up {@code TestApp}'s handler from the
     * neighbouring package and bind a second queue.
     */
    @Configuration
    @EnableAutoConfiguration
    static class App {

        @Bean
        SolaceListenerErrorHandler notThisOne() {
            return (request, exception) -> Map.of("handledBy", "notThisOne");
        }

        @Bean
        SolaceListenerErrorHandler chosen() {
            return (request, exception) -> Map.of("handledBy", "chosen");
        }

        @Bean
        FailingHandler failingHandler() { return new FailingHandler(); }
    }

    static class FailingHandler {

        @SolaceListener(id = "errh", queue = QUEUE, topics = TOPICS,
                concurrency = "1", ackMode = "CLIENT", errorHandler = "chosen")
        @SendTo
        public Map<String, Object> alwaysThrows(@Payload Map<String, Object> body) {
            throw new IllegalStateException("deliberate failure for " + body);
        }
    }
}
