package com.solace.samples.requestreply.core;

/**
 * Captures and restores observability context, and optionally carries it in messages, without the
 * core taking a hard dependency on OpenTelemetry.
 *
 * <h2>Why capture and restore at all</h2>
 * Thread propagation and <em>parent attribution</em> are different problems, and executor
 * instrumentation only solves the first. {@code future.complete()} runs its dependents on the
 * thread that completed it, where the current context describes the <em>reply</em>. The span that
 * should parent the continuation is the one active when the request was issued. Capturing at
 * request time and restoring at completion is what keeps causality right — and a trace that is
 * connected but wrongly parented is harder to notice than one that is broken outright.
 *
 * <p>Every method has a no-op default, so {@link #NOOP} is a complete implementation and tracing
 * stays genuinely optional rather than merely disabled.
 */
public interface TracingContextBridge {

    /** Captures the current context, or {@code null} when there is nothing to capture. */
    Object captureCurrent();

    /** Wraps {@code task} so it runs with {@code context} current. */
    Runnable wrap(Object context, Runnable task);

    /**
     * Writes the current trace context into an outbound JCSMP message so the next process can
     * continue the same trace. No-op by default.
     */
    default void inject(Object jcsmpMessage) { }

    /**
     * Reads trace context from an inbound JCSMP message and returns it as a captured context.
     * Returns {@code null} by default, which leaves the consumer's span unparented.
     */
    default Object extract(Object jcsmpMessage) { return null; }

    /** True when this bridge actually does something. Reported by diagnostics. */
    default boolean isActive() { return false; }

    /** Used when tracing is disabled or its dependencies are absent. */
    TracingContextBridge NOOP = new TracingContextBridge() {
        @Override
        public Object captureCurrent() { return null; }

        @Override
        public Runnable wrap(Object context, Runnable task) { return task; }
    };
}
