package com.solace.samples.requestreply.tracing;

import com.solace.samples.requestreply.config.TracingProperties;
import com.solace.samples.requestreply.core.TracingContextBridge;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.context.propagation.TextMapSetter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * OpenTelemetry implementation of the tracing seam.
 *
 * <p>Two jobs, and they solve different problems:
 *
 * <ul>
 *   <li><b>capture and restore</b> — fixes parent attribution across the completion executor.
 *       {@code Context.current()} is a thread-local, and the thread that completes a reply future
 *       holds the <em>reply's</em> context, not the request's. Without this the trace is connected
 *       but wrongly parented.</li>
 *   <li><b>inject and extract</b> — carries W3C trace context in the message so requestor and
 *       replier appear in one trace rather than two.</li>
 * </ul>
 *
 * <p>The message carriers come from Solace's JCSMP integration and are looked up reflectively, so
 * this class still loads — and still does the useful half — when that optional jar is absent.
 */
public class OpenTelemetryTracingBridge implements TracingContextBridge {

    private static final Logger log = LoggerFactory.getLogger(OpenTelemetryTracingBridge.class);

    private final OpenTelemetry openTelemetry;
    private final boolean propagate;
    private final TextMapSetter<Object> setter;
    private final TextMapGetter<Object> getter;

    @SuppressWarnings("unchecked")
    public OpenTelemetryTracingBridge(OpenTelemetry openTelemetry, TracingProperties props) {
        this.openTelemetry = openTelemetry;
        TextMapSetter<Object> s = null;
        TextMapGetter<Object> g = null;
        if (props.isPropagateContext()) {
            try {
                s = (TextMapSetter<Object>) Class
                        .forName("com.solace.messaging.trace.propagation.SolaceJCSMPTextMapSetter")
                        .getDeclaredConstructor().newInstance();
                g = (TextMapGetter<Object>) Class
                        .forName("com.solace.messaging.trace.propagation.SolaceJCSMPTextMapGetter")
                        .getDeclaredConstructor().newInstance();
                log.info("Tracing enabled with cross-process context propagation");
            } catch (ReflectiveOperationException | RuntimeException e) {
                log.warn("Tracing enabled, but com.solace:solace-opentelemetry-jcsmp-integration is "
                        + "not on the classpath. Context will be restored correctly within this "
                        + "process, but traces will not join up between requestor and replier. "
                        + "Add that dependency, or set "
                        + "solace.request-reply.tracing.propagate-context=false to silence this.");
            }
        } else {
            log.info("Tracing enabled, cross-process propagation disabled by configuration");
        }
        this.setter = s;
        this.getter = g;
        this.propagate = s != null && g != null;
    }

    @Override
    public Object captureCurrent() {
        return Context.current();
    }

    @Override
    public Runnable wrap(Object context, Runnable task) {
        // Context.wrap pins this specific captured context, as opposed to taskWrapping which
        // captures whatever is current at submission — here the captured one is the point.
        return context instanceof Context c ? c.wrap(task) : task;
    }

    @Override
    public void inject(Object jcsmpMessage) {
        if (!propagate || jcsmpMessage == null) { return; }
        try {
            openTelemetry.getPropagators().getTextMapPropagator()
                    .inject(Context.current(), jcsmpMessage, setter);
        } catch (RuntimeException e) {
            // Never let instrumentation break the message path.
            log.debug("Could not inject trace context", e);
        }
    }

    @Override
    public Object extract(Object jcsmpMessage) {
        if (!propagate || jcsmpMessage == null) { return null; }
        try {
            return openTelemetry.getPropagators().getTextMapPropagator()
                    .extract(Context.current(), jcsmpMessage, getter);
        } catch (RuntimeException e) {
            log.debug("Could not extract trace context", e);
            return null;
        }
    }

    @Override
    public boolean isActive() { return true; }
}
