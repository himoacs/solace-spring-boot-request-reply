package com.solace.samples.requestreply.core;

/**
 * Captures and restores whatever observability context is in play, without the core taking a
 * hard dependency on OpenTelemetry.
 *
 * <p>Why this exists at all: thread propagation and <em>parent attribution</em> are different
 * problems, and executor instrumentation only solves the first. {@code future.complete()} runs
 * its dependents on the thread that completed it, where the current context describes the
 * <em>reply</em>. The span that should parent the continuation is the one active when the
 * request was issued. Capturing at request time and restoring at completion is what keeps the
 * causality right — a trace that is merely connected but wrongly parented is harder to notice
 * than one that is broken.
 *
 * <p>The no-op implementation is the default, so tracing stays genuinely optional.
 */
public interface TracingContextBridge {

    /** Captures the current context, or {@code null} when there is nothing to capture. */
    Object captureCurrent();

    /** Wraps {@code task} so it runs with {@code context} current. */
    Runnable wrap(Object context, Runnable task);

    /** Bridge used when no tracing implementation is present. */
    TracingContextBridge NOOP = new TracingContextBridge() {
        @Override
        public Object captureCurrent() { return null; }

        @Override
        public Runnable wrap(Object context, Runnable task) { return task; }
    };
}
