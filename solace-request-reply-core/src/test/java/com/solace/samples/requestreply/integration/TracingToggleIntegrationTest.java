package com.solace.samples.requestreply.integration;

import com.solace.samples.requestreply.core.TracingContextBridge;
import com.solace.samples.requestreply.support.SolaceTestBroker;
import com.solace.samples.requestreply.support.TestApp;
import com.solace.samples.requestreply.tracing.OpenTelemetryTracingBridge;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tracing must be genuinely optional, not merely switchable.
 *
 * <p>A customer who wants nothing to do with tracing should pay nothing for it: no active bridge,
 * no capture on the request path, no wrapping on the completion path. Since the OpenTelemetry API
 * is on the test classpath, only the configuration flag separates the two cases here — which is
 * exactly the condition worth asserting, because it is the one a customer controls.
 */
class TracingToggleIntegrationTest {

    static void broker(DynamicPropertyRegistry registry) {
        registry.add("solace.java.host", SolaceTestBroker::smfHost);
        registry.add("solace.java.msg-vpn", SolaceTestBroker::vpn);
        registry.add("solace.java.client-username", SolaceTestBroker::username);
        registry.add("solace.java.client-password", SolaceTestBroker::password);
    }

    private static final String[] BASE = {
            "test.queue=q.test.tracing",
            "test.subscription=test/tracing/request/v1/>",
            "test.concurrency=1",
            "solace.request-reply.reply.topic-pattern=test/tracing/reply/v1/{instanceId}",
            "solace.request-reply.reply.queue-name-pattern=q.test.tracing.reply.{instanceId}",
            "solace.request-reply.replier.queue=q.test.tracing",
            "solace.request-reply.replier.topics=test/tracing/request/v1/>",
            "solace.request-reply.replier.concurrency=1"
    };

    @Nested
    @SpringBootTest(classes = TestApp.class,
            webEnvironment = SpringBootTest.WebEnvironment.NONE,
            properties = {
                    "test.queue=q.test.tracing.off",
                    "test.subscription=test/tracingoff/request/v1/>",
                    "test.concurrency=1",
                    "solace.request-reply.reply.topic-pattern=test/tracingoff/reply/v1/{instanceId}",
                    "solace.request-reply.reply.queue-name-pattern=q.test.tracingoff.reply.{instanceId}",
                    "solace.request-reply.replier.queue=q.test.tracing.off",
                    "solace.request-reply.replier.topics=test/tracingoff/request/v1/>",
                    "solace.request-reply.replier.concurrency=1"
                    // tracing.enabled deliberately absent: the default must be off.
            })
    class DisabledByDefault {

        @DynamicPropertySource
        static void props(DynamicPropertyRegistry registry) { broker(registry); }

        @Autowired TracingContextBridge bridge;

        @Test
        void usesTheNoOpBridge() {
            assertThat(bridge)
                    .as("with no configuration, tracing must not be wired at all")
                    .isSameAs(TracingContextBridge.NOOP);
            assertThat(bridge.isActive()).isFalse();
            assertThat(bridge.captureCurrent()).isNull();
            Runnable task = () -> { };
            assertThat(bridge.wrap(null, task))
                    .as("the no-op bridge must not even allocate a wrapper")
                    .isSameAs(task);
        }
    }

    @Nested
    @SpringBootTest(classes = TestApp.class,
            webEnvironment = SpringBootTest.WebEnvironment.NONE,
            properties = {
                    "test.queue=q.test.tracing.on",
                    "test.subscription=test/tracingon/request/v1/>",
                    "test.concurrency=1",
                    "solace.request-reply.reply.topic-pattern=test/tracingon/reply/v1/{instanceId}",
                    "solace.request-reply.reply.queue-name-pattern=q.test.tracingon.reply.{instanceId}",
                    "solace.request-reply.replier.queue=q.test.tracing.on",
                    "solace.request-reply.replier.topics=test/tracingon/request/v1/>",
                    "solace.request-reply.replier.concurrency=1",
                    "solace.request-reply.tracing.enabled=true"
            })
    class EnabledByConfiguration {

        @DynamicPropertySource
        static void props(DynamicPropertyRegistry registry) { broker(registry); }

        @Autowired TracingContextBridge bridge;

        @Test
        void usesTheOpenTelemetryBridge() {
            assertThat(bridge).isInstanceOf(OpenTelemetryTracingBridge.class);
            assertThat(bridge.isActive()).isTrue();
            assertThat(bridge.captureCurrent())
                    .as("an active bridge must capture something to restore later")
                    .isNotNull();
        }
    }
}
