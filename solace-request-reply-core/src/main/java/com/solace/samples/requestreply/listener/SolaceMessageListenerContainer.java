package com.solace.samples.requestreply.listener;

import com.solace.samples.requestreply.api.RequestReplyMessage;
import com.solace.samples.requestreply.api.SolaceHeaders;
import com.solace.samples.requestreply.api.SolaceListenerErrorHandler;
import com.solace.samples.requestreply.core.PayloadCodec;
import com.solace.samples.requestreply.core.TracingContextBridge;
import com.solace.samples.requestreply.endpoint.RequestQueueProvisioner;
import com.solace.samples.requestreply.transport.FlowConsumer;
import com.solace.samples.requestreply.transport.InboundMessage;
import com.solace.samples.requestreply.transport.PersistentPublisher;
import com.solace.samples.requestreply.transport.PublishTicket;
import com.solace.samples.requestreply.transport.SolaceSession;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.Queue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 * Binds N flows over one queue and dispatches each request to the handler.
 *
 * <h2>Why a queue and not a topic subscription</h2>
 * A direct topic subscription fans out: every replier instance would receive every request,
 * execute the handler, and publish a reply. For a booking system that means one request
 * reserving N seats, with the requestor silently discarding N-1 duplicate replies. A
 * non-exclusive queue with competing consumers is what makes scale-out safe.
 *
 * <h2>Acknowledgement ordering</h2>
 * The request is acknowledged only after the reply has been published <em>and</em> the broker
 * has acknowledged that publish. The alternative — ack first, then reply — loses the request
 * entirely if the process dies in between, leaving the work done but the caller told it failed,
 * with nothing left to redeliver. Acking last means a crash produces a redelivery instead, which
 * an idempotent handler absorbs.
 */
public class SolaceMessageListenerContainer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(SolaceMessageListenerContainer.class);

    private final SolaceListenerEndpoint endpoint;
    private final SolaceSession session;
    private final RequestQueueProvisioner provisioner;
    private final PersistentPublisher publisher;
    private final PayloadCodec codec;
    private final HandlerMethodInvoker invoker;
    private final SolaceListenerErrorHandler errorHandler;
    private final ExecutorService handlerExecutor;
    private final TracingContextBridge tracing;
    private final boolean replyDmqEligible;
    /** Resolved once at construction, not per message: "unset" already means request.timeout. */
    private final long replyTtlMillis;

    private final List<FlowConsumer> flows = new ArrayList<>();
    private volatile Queue queue;
    private volatile boolean running;

    public SolaceMessageListenerContainer(SolaceListenerEndpoint endpoint,
                                          SolaceSession session,
                                          RequestQueueProvisioner provisioner,
                                          PersistentPublisher publisher,
                                          PayloadCodec codec,
                                          HandlerMethodInvoker invoker,
                                          SolaceListenerErrorHandler errorHandler,
                                          ExecutorService handlerExecutor,
                                          TracingContextBridge tracing,
                                          boolean replyDmqEligible,
                                          long replyTtlMillis) {
        this.endpoint = endpoint;
        this.session = session;
        this.provisioner = provisioner;
        this.publisher = publisher;
        this.codec = codec;
        this.invoker = invoker;
        this.errorHandler = errorHandler;
        this.handlerExecutor = handlerExecutor;
        this.tracing = tracing;
        this.replyDmqEligible = replyDmqEligible;
        this.replyTtlMillis = replyTtlMillis;
    }

    public synchronized void start() {
        if (running) { return; }
        queue = provisioner.ensure(endpoint.queue(), endpoint.topics());
        for (int i = 0; i < endpoint.concurrency(); i++) {
            FlowConsumer flow = new FlowConsumer(session, queue,
                    endpoint.id() + "-" + i, endpoint.clientAck(), this::onRequest);
            flow.start();
            flows.add(flow);
        }
        session.onReconnect(this::onReconnect);
        running = true;
        log.info("Listener '{}' started: queue={} concurrency={} topics={}",
                endpoint.id(), endpoint.queue(), endpoint.concurrency(), endpoint.topics());
    }

    private void onReconnect() {
        if (!running) { return; }
        try {
            queue = provisioner.ensure(endpoint.queue(), endpoint.topics());
            for (FlowConsumer flow : flows) { flow.rebind(); }
            log.info("Listener '{}' rebound after reconnect", endpoint.id());
        } catch (RuntimeException ex) {
            log.error("Listener '{}' could not rebind after reconnect", endpoint.id(), ex);
        }
    }

    /**
     * Dispatch. Runs on the JCSMP flow thread, so the handler itself is handed to an
     * application pool: business logic on the dispatch thread would stall every other request
     * on this instance, and a nested request from it could never complete.
     */
    private void onRequest(BytesXMLMessage raw) {
        // Prefer the context carried in the message: it makes the handler a child of the span
        // that issued the request, in the other process. Falling back to the dispatch thread's
        // context would produce a valid trace rooted in the wrong place.
        Object ctx = tracing.extract(raw);
        if (ctx == null) { ctx = tracing.captureCurrent(); }
        Object finalCtx = ctx;
        handlerExecutor.execute(tracing.wrap(finalCtx, () -> handle(raw)));
    }

    private void handle(BytesXMLMessage raw) {
        RequestReplyMessage request = InboundMessage.toModel(raw);
        long handlerStart = System.nanoTime();
        Object result = null;
        RequestReplyMessage reply;
        try {
            result = invoker.invoke(endpoint, request);
            reply = successReply(request, result);
        } catch (Exception ex) {
            Throwable cause = ex instanceof InvocationTargetException ite && ite.getCause() != null
                    ? ite.getCause() : ex;
            reply = errorReply(request, cause, handlerStart);
        }
        long handlerNanos = System.nanoTime() - handlerStart;

        if (reply == null) {
            acknowledge(raw);
            return;
        }
        reply.addHeader(SolaceHeaders.HANDLER_NANOS, Long.toString(handlerNanos));
        reply.addHeader(SolaceHeaders.REPLY_SENT_AT, Long.toString(System.currentTimeMillis() * 1_000));

        String destination = endpoint.replyTo() != null && !endpoint.replyTo().isBlank()
                ? endpoint.replyTo() : request.getReplyTo();
        if (destination == null || destination.isBlank()) {
            log.warn("Request correlationId={} carries no reply-to and no @SendTo destination is "
                    + "configured; dropping the reply", request.getCorrelationId());
            acknowledge(raw);
            return;
        }

        PublishTicket ticket = new PublishTicket(request.getCorrelationId(), destination, System.nanoTime());
        // A reply outlives its usefulness the moment the requestor's future gives up, so it
        // carries a TTL and dead-letters rather than accumulating in an orphaned reply queue.
        reply.setDmqEligible(replyDmqEligible);
        try {
            publisher.publish(destination, reply, ticket, replyTtlMillis);
            // Ack only once the broker confirms the reply is spooled. Anything earlier risks
            // losing the request while the work is already done.
            ticket.sendFuture().whenComplete((res, err) -> {
                if (err == null) {
                    acknowledge(raw);
                } else {
                    log.error("Reply for correlationId={} was not spooled; leaving the request "
                                    + "unacknowledged so it is redelivered",
                            request.getCorrelationId(), err);
                }
            });
        } catch (RuntimeException ex) {
            log.error("Could not publish the reply for correlationId={}; leaving the request "
                    + "unacknowledged", request.getCorrelationId(), ex);
        }
    }

    private RequestReplyMessage successReply(RequestReplyMessage request, Object result) {
        if (!endpoint.sendReply() || result == null) { return null; }
        if (result instanceof RequestReplyMessage m) {
            m.setCorrelationId(request.getCorrelationId());
            return m;
        }
        return request.newReply().setPayload(codec.serialize(result));
    }

    private RequestReplyMessage errorReply(RequestReplyMessage request, Throwable cause, long start) {
        if (errorHandler != null) {
            try {
                Object handled = errorHandler.handleError(request,
                        cause instanceof Exception e ? e : new RuntimeException(cause));
                if (handled != null) { return successReply(request, handled); }
            } catch (Exception rethrown) {
                cause = rethrown;
            }
        }
        log.warn("Handler for correlationId={} failed; forwarding as an error reply so the "
                + "requestor fails fast rather than waiting out its timeout",
                request.getCorrelationId(), cause);
        String msg = cause.getMessage() == null ? cause.getClass().getSimpleName() : cause.getMessage();
        return request.newReply().asError(msg);
    }

    private void acknowledge(BytesXMLMessage raw) {
        if (endpoint.clientAck()) { raw.ackMessage(); }
    }

    public boolean isRunning() { return running; }

    public String id() { return endpoint.id(); }

    @Override
    public synchronized void close() {
        running = false;
        for (FlowConsumer f : flows) { f.close(); }
        flows.clear();
    }
}
