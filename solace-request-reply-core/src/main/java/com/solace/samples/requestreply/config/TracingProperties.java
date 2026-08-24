package com.solace.samples.requestreply.config;

/**
 * Tracing switches, under {@code solace.request-reply.tracing}.
 *
 * <p>Off by default and inert when the OpenTelemetry libraries are absent, so tracing is opt-in
 * twice over: a customer who wants nothing to do with it changes no configuration and carries no
 * dependency.
 */
public class TracingProperties {

    /**
     * Master switch. When false the library uses a no-op bridge that captures nothing and wraps
     * nothing, with no measurable cost.
     */
    private boolean enabled = false;

    /**
     * Carry W3C trace context in published messages so a trace spans processes.
     *
     * <p>Requires {@code com.solace:solace-opentelemetry-jcsmp-integration}. Without it,
     * context is still restored correctly <em>within</em> this process — which is the part that
     * fixes parent attribution across the completion executor — but traces will not join up
     * between requestor and replier.
     */
    private boolean propagateContext = true;

    /** Instrumentation scope name reported to the tracing backend. */
    private String instrumentationName = "com.solace.samples.requestreply";

    public boolean isEnabled() { return enabled; }
    public void setEnabled(boolean enabled) { this.enabled = enabled; }
    public boolean isPropagateContext() { return propagateContext; }
    public void setPropagateContext(boolean v) { this.propagateContext = v; }
    public String getInstrumentationName() { return instrumentationName; }
    public void setInstrumentationName(String v) { this.instrumentationName = v; }
}
