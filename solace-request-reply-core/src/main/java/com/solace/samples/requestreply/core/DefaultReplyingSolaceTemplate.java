package com.solace.samples.requestreply.core;

import com.solace.samples.requestreply.api.ReplyingSolaceTemplate;
import com.solace.samples.requestreply.api.RequestReplyFuture;
import com.solace.samples.requestreply.api.RequestReplyMessage;
import com.solace.samples.requestreply.config.SolaceRequestReplyProperties;
import com.solace.samples.requestreply.endpoint.ReplyEndpoint;
import com.solace.samples.requestreply.exception.RemoteErrorException;
import com.solace.samples.requestreply.exception.RequestReplyException;
import com.solace.samples.requestreply.exception.RequestTimeoutException;
import com.solace.samples.requestreply.exception.TransportException;
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

    private final AtomicLong sequence = new AtomicLong();
    private final Map<String, Expression> replyPlaceholderExpressions = new java.util.LinkedHashMap<>();

    private volatile FlowConsumer replyFlow;

    public DefaultReplyingSolaceTemplate(SolaceSession session,
                                         ReplyEndpoint replyEndpoint,
                                         PersistentPublisher publisher,
                                         CorrelationStore store,
                                         SolaceRequestReplyProperties props,
                                         PayloadCodec codec,
                                         ExecutorService completionExecutor) {
        this.session = session;
        this.replyEndpoint = replyEndpoint;
        this.publisher = publisher;
        this.store = store;
        this.props = props;
        this.codec = codec;
        this.completionExecutor = completionExecutor;
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
        // Provisions the durable queue and applies its subscription. Both happen before the flow
        // binds, so there is no window in which the endpoint exists but matches nothing.
        replyEndpoint.establish();
        replyFlow = new FlowConsumer(session, replyEndpoint.queue(), "reply", false, this::onReplyMessage);
        replyFlow.start();
        reaper.start();
        // The reply-to template, not a concrete reply-to: per-request levels are wildcards here
        // and are filled in per request, so this string equals the subscription by construction.
        log.info("Request/reply template ready. replyTo={} queue={}",
                replyEndpoint.subscription(), replyEndpoint.queue().getName());
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
    public String replyTopicPattern() { return replyEndpoint.subscription(); }

    @Override
    public Duration defaultReplyTimeout() { return props.getRequest().getTimeout(); }

    // ------------------------------------------------------------------ sending

    @Override
    public <T, R> RequestReplyFuture<R> sendAndReceive(String topic, T payload,
                                                       Class<R> replyType, Duration timeout) {
        return sendAndReceive(topic, payload, replyType, timeout, null);
    }

    @Override
    public <T, R> RequestReplyFuture<R> sendAndReceive(String topic, T payload,
                                                       Class<R> replyType, Duration timeout,
                                                       String correlationId) {
        RequestReplyMessage request = new RequestReplyMessage(codec.serialize(payload));
        if (correlationId != null && !correlationId.isBlank()) {
            request.setCorrelationId(correlationId);
        }
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

        PendingRequest pending = new PendingRequest(correlationId, topic,
                System.currentTimeMillis() + timeout.toMillis(), timeout.toMillis(), future);
        // Register BEFORE publishing: a fast replier on the same broker can return a reply
        // before the publish acknowledgement arrives, and registering afterwards would drop it.
        store.register(pending);

        ticket.sendFuture().whenComplete((res, err) -> {
            if (err != null) {
                // No point waiting out the timeout for a reply to a request that never landed.
                store.remove(correlationId).ifPresent(p -> completeExceptionally(p, err));
            }
        });

        long ttl = props.getRequest().isTtlMatchesTimeout() ? timeout.toMillis() : 0L;
        request.setDmqEligible(props.getDmq().isEnabled() && props.getRequest().isDmqEligible());
        try {
            publisher.publish(topic, request, ticket, ttl);
        } catch (TransportException e) {
            store.remove(correlationId).ifPresent(p -> completeExceptionally(p, e));
        }
        return future;
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
        RequestReplyMessage reply = InboundMessage.toModel(msg);
        String correlationId = reply.getCorrelationId();

        store.remove(correlationId).ifPresentOrElse(pending ->
                // Off the dispatch thread: completing here would run every dependent stage of
                // the caller's future on JCSMP's, stalling delivery for every other reply.
                completionExecutor.execute(() -> {
                    if (reply.isError()) {
                        pending.getFuture().completeExceptionally(
                                new RemoteErrorException(correlationId, reply.getErrorMessage()));
                    } else {
                        pending.getFuture().complete(reply);
                    }
                }),
                () -> log.debug("Uncorrelated reply correlationId={} — already timed out, a "
                        + "duplicate, or addressed to a previous incarnation of this instance",
                        correlationId));
    }

    private void expire(PendingRequest pending) {
        completeExceptionally(pending, new RequestTimeoutException(
                pending.getCorrelationId(), pending.getRequestTopic(),
                Duration.ofMillis(pending.getTimeoutMs())));
    }

    private void completeExceptionally(PendingRequest pending, Throwable cause) {
        completionExecutor.execute(() -> pending.getFuture().completeExceptionally(cause));
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
        // Anything still waiting for a reply gets told plainly rather than left to hang or time
        // out with no explanation. store.remove(id) is what CorrelationStore documents as the
        // ownership decision, so a reply that is genuinely mid-flight and wins that race still
        // completes normally instead of being cut off here.
        for (PendingRequest p : store.pending()) {
            store.remove(p.getCorrelationId()).ifPresent(owned ->
                    completeExceptionally(owned, new RequestReplyException(
                            "Template is shutting down; correlationId=" + owned.getCorrelationId()
                                    + " will not receive a reply")));
        }
        if (replyFlow != null) { replyFlow.close(); }
        replyEndpoint.close();
        // publisher and completionExecutor are not ours to close: both are beans with their own
        // destroyMethod, and the publisher is shared with every listener container -- closing it
        // here raced against their shutdown with no ordering guarantee between the two beans.
    }
}
