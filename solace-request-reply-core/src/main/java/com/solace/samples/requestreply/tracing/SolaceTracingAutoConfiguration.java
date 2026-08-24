package com.solace.samples.requestreply.tracing;

import com.solace.samples.requestreply.config.SolaceRequestReplyAutoConfiguration;
import com.solace.samples.requestreply.config.SolaceRequestReplyProperties;
import com.solace.samples.requestreply.core.TracingContextBridge;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;

/**
 * Activates tracing only when it is both asked for and possible.
 *
 * <p>Three independent conditions have to hold, which is what makes this safe to leave in the
 * library for customers who want nothing to do with tracing:
 * <ol>
 *   <li>{@code solace.request-reply.tracing.enabled=true} — off by default;</li>
 *   <li>the OpenTelemetry API on the classpath — an optional dependency;</li>
 *   <li>no {@link TracingContextBridge} already defined, so an application can substitute its own.</li>
 * </ol>
 *
 * <p>Fail any of them and the core keeps {@link TracingContextBridge#NOOP}, which captures nothing
 * and wraps nothing.
 */
// Must run BEFORE the core auto-configuration. Both define a TracingContextBridge conditionally
// on one being absent, so whichever runs first wins — and if the core ran first its no-op bridge
// would silently defeat tracing.enabled=true.
@AutoConfiguration(before = SolaceRequestReplyAutoConfiguration.class)
@ConditionalOnClass({OpenTelemetry.class, io.opentelemetry.context.Context.class})
@ConditionalOnProperty(prefix = "solace.request-reply.tracing", name = "enabled", havingValue = "true")
public class SolaceTracingAutoConfiguration {

    /**
     * Prefers an application-provided {@link OpenTelemetry} bean — which is what Spring Boot's own
     * observability auto-configuration or the OTel Spring starter supplies — and falls back to the
     * global instance the Java agent installs.
     */
    @Bean
    @ConditionalOnMissingBean(TracingContextBridge.class)
    public TracingContextBridge solaceTracingContextBridge(
            ObjectProvider<OpenTelemetry> openTelemetry, SolaceRequestReplyProperties props) {
        OpenTelemetry otel = openTelemetry.getIfAvailable(GlobalOpenTelemetry::get);
        return new OpenTelemetryTracingBridge(otel, props.getTracing());
    }
}
