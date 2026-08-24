package com.solace.samples.requestreply.core;

import com.solace.samples.requestreply.api.PublishResult;
import com.solace.samples.requestreply.api.ReplyingSolaceTemplate;
import com.solace.samples.requestreply.api.RequestReplyFuture;
import com.solace.samples.requestreply.api.RequestReplyMessage;
import com.solace.samples.requestreply.api.SolaceHeaders;
import com.solace.samples.requestreply.config.SolaceRequestReplyProperties;
import com.solace.samples.requestreply.endpoint.ReplyEndpoint;
import com.solace.samples.requestreply.exception.RemoteErrorException;
import com.solace.samples.requestreply.exception.RequestTimeoutException;
import com.solace.samples.requestreply.exception.TransportException;
import com.solace.samples.requestreply.latency.LatencyRecorder;
import com.solace.samples.requestreply.latency.LatencySample;
import com.solace.samples.requestreply.transport.FlowConsumer;
import com.solace.samples.requestreply.transport.InboundMessage;
import com.solace.samples.requestreply.transport.PersistentPublisher;
import com.solace.samples.requestreply.transport.PublishTicket;
import com.solace.samples.requestreply.transport.SolaceSession;
import com.solacesystems.jcsmp.BytesXMLMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * Requestor implementation: publish, correlate, complete.
 *
 * <h2>Threading</h2>
 * Replies arrive on the JCSMP dispatch thread. Completing the future there would run every
 * caller-registered continuation on it, and the guidance is explicit that listener callbacks
 * must return promptly — a slow {@code thenApply} would stall every other reply on this
 * instance. So completion is handed to a bounded application pool.
 *
 * <p>That pool is deliberately a plain JDK {@code ThreadPoolExecutor}. The OpenTelemetry agent
 * propagates context across executors by an exact class-name allowlist, and the JDK pools are
 * on it while a custom {@code Executor} would not be — so the ordinary choice is also the one
 * that keeps traces intact.
 */
public class DefaultReplyingSolaceTemplate implements ReplyingSolaceTemplate, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(DefaultReplyingSolaceTemplate.class);
    private static final SpelExpressionParser SPEL = new SpelExpressionParser();
    /** Internal header prefix; stripped before publishing so it never reaches the wire. */
    private static final String PLACEHOLDER_HEADER = "rr_rt_";

    private final SolaceSession session;
    private final ReplyEndpoint replyEndpoint;
    private final PersistentPublisher publisher;
    private final CorrelationStore store;
    private final TimeoutReaper reaper;
    private final SolaceRequestReplyProperties props;
    private final PayloadCodec codec;
    private final ExecutorService completionExecutor;
    private final LatencyRecorder latency;
    private final TracingContextBridge tracing;

    private final AtomicLong sequence = new AtomicLong();
    private final Map<String, Long> publishConfirmNanos = new java.util.concurrent.ConcurrentHashMap<>();
    private final Expression partitionKeyExpression;
    private final Map<String, Expression> replyPlaceholderExpressions = new java.util.LinkedHashMap<>();

    private volatile FlowConsumer replyFlow;

    public DefaultReplyingSolaceTemplate(SolaceSession session,
                                         ReplyEndpoint replyEndpoint,
                                         PersistentPublisher publisher,
                                         CorrelationStore store,
                                         SolaceRequestReplyProperties props,
                                         PayloadCodec codec,
                                         ExecutorService completionExecutor,
                                         LatencyRecorder latency,
                                         TracingContextBridge tracing) {
        this.session = session;
        this.replyEndpoint = replyEndpoint;
        this.publisher = publisher;
        this.store = store;
        this.props = props;
        this.codec = codec;
        this.completionExecutor = completionExecutor;
        this.latency = latency;
        this.tracing = tracing;
        String expr = props.getRequest().getPartitionKeyExpression();
        this.partitionKeyExpression = (expr == null || expr.isBlank()) ? null : SPEL.parseExpression(expr);
        props.getReply().getPerRequestPlaceholderExpressions().forEach((name, spel) -> {
            if (spel != null && !spel.isBlank()) {
                replyPlaceholderExpressions.put(name, SPEL.parseExpression(spel));
            }
        });
        this.reaper = new TimeoutReaper(store, 100, this::expire);
    }

    // ---------------------------------------------------------------- lifecycle

    public void start() {
        publisher.start();
        replyEndpoint.establish();
        bindReplyFlow();
        reaper.start();
        session.onReconnect(this::onReconnect);
        log.info("Request/reply template ready. replyTopic={} subscription={}",
                replyEndpoint.replyTopic(Map.of()), replyEndpoint.subscription());
    }

    private void bindReplyFlow() {
        replyFlow = new FlowConsumer(session, replyEndpoint.queue(), "reply", false, this::onReplyMessage);
        replyFlow.start();
        // Only now does the queue exist on the broker, so only now can it carry a subscription.
        replyEndpoint.applySubscription();
    }

    /**
     * Recovery after a reconnect: re-establish the endpoint, then rebind.
     *
     * <p>Load-bearing for a temporary endpoint, which the broker recreates without its
     * subscription once the linger window expires. Without this the session is up, the flow is
     * bound, nothing logs an error, and every request times out until someone restarts the pod.
     */
    private void onReconnect() {
        if (!props.getReply().isRecreateOnReconnect()) { return; }
        try {
            replyEndpoint.reestablish();
            if (replyFlow != null) { replyFlow.close(); }
            bindReplyFlow();
            log.info("Reply path re-established after reconnect");
        } catch (RuntimeException ex) {
            log.error("Reply path could not be re-established; this instance can no longer "
                    + "receive replies and should be considered unhealthy", ex);
        }
    }

    @Override
    public boolean waitForReplyEndpoint(Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            if (replyEndpoint.isEstablished() && replyFlow != null && replyFlow.isBound()) { return true; }
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return replyEndpoint.isEstablished() && replyFlow != null && replyFlow.isBound();
    }

    @Override
    public String replyTopic() { return replyEndpoint.replyTopic(Map.of()); }

    @Override
    public Duration defaultReplyTimeout() { return props.getRequest().getTimeout(); }

    // ------------------------------------------------------------------ sending

    @Override
    public <T, R> RequestReplyFuture<R> sendAndReceive(String topic, String partitionKey, T payload,
                                                       Class<R> replyType, Duration timeout) {
        return sendAndReceive(topic, partitionKey, payload, replyType, timeout, null);
    }

    @Override
    public <T, R> RequestReplyFuture<R> sendAndReceive(String topic, String partitionKey, T payload,
                                                       Class<R> replyType, Duration timeout,
                                                       String correlationId) {
        RequestReplyMessage request = new RequestReplyMessage(codec.serialize(payload));
        if (correlationId != null && !correlationId.isBlank()) {
            request.setCorrelationId(correlationId);
        }
        request.setPartitionKey(partitionKey != null ? partitionKey : derivePartitionKey(payload));
        // Reply-topic placeholders are derived here, where the typed payload is still available;
        // by the time the raw path runs the payload is opaque bytes.
        replyPlaceholderExpressions.forEach((name, expr) -> {
            String v = evaluate(expr, payload);
            if (v != null) { request.addHeader(PLACEHOLDER_HEADER + name, v); }
        });
        RequestReplyFuture<RequestReplyMessage> raw = sendAndReceive(topic, request, timeout);
        return map(raw, msg -> codec.deserialize(msg.getPayload(), replyType));
    }

    @Override
    public RequestReplyFuture<RequestReplyMessage> sendAndReceive(String topic,
                                                                  RequestReplyMessage request,
                                                                  Duration timeout) {
        long startNanos = System.nanoTime();
        String correlationId = request.getCorrelationId() != null
                ? request.getCorrelationId() : UUID.randomUUID().toString();

        Map<String, String> perRequest = perRequestPlaceholderValues(request);
        request.setCorrelationId(correlationId);
        request.setReplyTo(replyEndpoint.replyTopic(perRequest));
        if (props.getRequest().isSequenceNumbers()) {
            request.setSequence(sequence.incrementAndGet());
        }

        PublishTicket ticket = new PublishTicket(correlationId, topic, startNanos);
        RequestReplyFuture<RequestReplyMessage> future = new RequestReplyFuture<>(ticket.sendFuture());

        // Register BEFORE publishing: a fast replier on the same broker can return a reply
        // before the publish acknowledgement arrives, and registering afterwards would drop it.
        store.register(new PendingRequest(correlationId, topic,
                System.currentTimeMillis() + timeout.toMillis(), timeout.toMillis(), startNanos,
                future, tracing.captureCurrent()));

        ticket.sendFuture().whenComplete((res, err) -> {
            if (res != null) {
                publishConfirmNanos.put(correlationId, res.confirmNanos());
            } else if (err != null) {
                // No point waiting out the timeout for a reply to a request that never landed.
                store.remove(correlationId).ifPresent(p -> {
                    latency.record(sampleFor(p, LatencySample.Outcome.PUBLISH_FAILURE, 0L, 0L));
                    completeExceptionally(p, err);
                });
            }
        });

        long ttl = props.getRequest().isTtlMatchesTimeout() ? timeout.toMillis() : 0L;
        try {
            publisher.publish(topic, request, ticket, ttl);
        } catch (TransportException e) {
            store.remove(correlationId).ifPresent(p -> completeExceptionally(p, e));
        }
        return future;
    }

    private String derivePartitionKey(Object payload) {
        if (partitionKeyExpression == null || payload == null) { return null; }
        try {
            Object v = partitionKeyExpression.getValue(payload);
            return v == null ? null : String.valueOf(v);
        } catch (RuntimeException ex) {
            log.warn("partition-key-expression failed against {}; publishing without a key. "
                    + "On a partitioned queue the broker will assign a random partition, which "
                    + "silently loses ordering.", payload.getClass().getSimpleName(), ex);
            return null;
        }
    }

    private Map<String, String> perRequestPlaceholderValues(RequestReplyMessage request) {
        Map<String, String> out = new HashMap<>();
        for (String name : props.getReply().getPerRequestPlaceholders()) {
            String v = request.getHeader(PLACEHOLDER_HEADER + name);
            if (v == null) { v = request.getHeader(name); }
            if (v != null) { out.put(name, v); }
        }
        return out;
    }

    private String evaluate(Expression expr, Object payload) {
        if (payload == null) { return null; }
        try {
            Object v = expr.getValue(payload);
            return v == null ? null : String.valueOf(v);
        } catch (RuntimeException ex) {
            log.warn("Reply-topic placeholder expression failed against {}; that level will "
                    + "render as 'unknown'", payload.getClass().getSimpleName(), ex);
            return null;
        }
    }

    // ---------------------------------------------------------------- receiving

    private void onReplyMessage(BytesXMLMessage msg) {
        long dispatchDelayNanos = 0L;
        RequestReplyMessage reply = InboundMessage.toModel(msg);
        String correlationId = reply.getCorrelationId();

        store.remove(correlationId).ifPresentOrElse(pending -> {
            long total = System.nanoTime() - pending.getStartNanos();
            long confirm = publishConfirmNanos.getOrDefault(correlationId, 0L);
            publishConfirmNanos.remove(correlationId);
            long handler = parseLong(reply.getHeader(SolaceHeaders.HANDLER_NANOS));

            LatencySample.Outcome outcome = reply.isError()
                    ? LatencySample.Outcome.REMOTE_ERROR : LatencySample.Outcome.SUCCESS;
            latency.record(new LatencySample(correlationId, outcome, total, confirm, handler,
                    dispatchDelayNanos, reply.getSequence()));

            // Off the dispatch thread, with the request's context restored so the trace's
            // parent is the span that issued the request, not the one that received the reply.
            completionExecutor.execute(tracing.wrap(pending.getTracingContext(), () -> {
                if (reply.isError()) {
                    pending.getFuture().completeExceptionally(
                            new RemoteErrorException(correlationId, reply.getErrorMessage()));
                } else {
                    pending.getFuture().complete(reply);
                }
            }));
        }, () -> log.debug("Uncorrelated reply correlationId={} — already timed out, a duplicate, "
                + "or addressed to a previous incarnation of this instance", correlationId));
    }

    private void expire(PendingRequest pending) {
        long confirm = publishConfirmNanos.getOrDefault(pending.getCorrelationId(), 0L);
        publishConfirmNanos.remove(pending.getCorrelationId());
        latency.record(sampleFor(pending, LatencySample.Outcome.TIMEOUT, confirm, 0L));
        completeExceptionally(pending, new RequestTimeoutException(
                pending.getCorrelationId(), pending.getRequestTopic(),
                Duration.ofMillis(pending.getTimeoutMs())));
    }

    private LatencySample sampleFor(PendingRequest p, LatencySample.Outcome outcome,
                                    long confirmNanos, long handlerNanos) {
        return new LatencySample(p.getCorrelationId(), outcome,
                System.nanoTime() - p.getStartNanos(), confirmNanos, handlerNanos, 0L, null);
    }

    private void completeExceptionally(PendingRequest pending, Throwable cause) {
        completionExecutor.execute(tracing.wrap(pending.getTracingContext(),
                () -> pending.getFuture().completeExceptionally(cause)));
    }

    private static long parseLong(String s) {
        if (s == null) { return 0L; }
        try {
            return Long.parseLong(s.trim());
        } catch (NumberFormatException e) {
            return 0L;
        }
    }

    /** Preserves the send future while mapping the reply payload. */
    private <R> RequestReplyFuture<R> map(RequestReplyFuture<RequestReplyMessage> source,
                                          Function<RequestReplyMessage, R> mapper) {
        RequestReplyFuture<R> mapped = new RequestReplyFuture<>(source.getSendFuture());
        source.whenComplete((msg, err) -> {
            if (err != null) {
                mapped.completeExceptionally(err instanceof java.util.concurrent.CompletionException ce
                        && ce.getCause() != null ? ce.getCause() : err);
            } else {
                try {
                    mapped.complete(mapper.apply(msg));
                } catch (RuntimeException ex) {
                    mapped.completeExceptionally(ex);
                }
            }
        });
        return mapped;
    }

    @Override
    public void close() {
        reaper.close();
        if (replyFlow != null) { replyFlow.close(); }
        replyEndpoint.close();
        publisher.close();
        completionExecutor.shutdown();
    }
}
