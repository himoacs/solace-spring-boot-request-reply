package com.solace.samples.requestreply.listener;

import com.solace.samples.requestreply.api.RequestReplyMessage;
import com.solace.samples.requestreply.api.RetryableHandlerException;
import com.solace.samples.requestreply.api.SolaceListenerErrorHandler;
import com.solace.samples.requestreply.config.SolaceRequestReplyProperties;
import com.solace.samples.requestreply.core.PayloadCodec;
import com.solace.samples.requestreply.endpoint.RequestQueueProvisioner;
import com.solace.samples.requestreply.transport.FlowConsumer;
import com.solace.samples.requestreply.transport.InboundMessage;
import com.solace.samples.requestreply.transport.PersistentPublisher;
import com.solace.samples.requestreply.transport.PublishTicket;
import com.solace.samples.requestreply.transport.SolaceSession;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.Queue;
import com.solacesystems.jcsmp.XMLMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

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
 *
 * <h2>One number, one meaning</h2>
 * JCSMP dispatches every flow in a session on a single shared thread — measured, not assumed:
 * four flows delivering a 400&nbsp;ms handler ran in ~4.8s, not the ~1.2s four-way parallelism
 * would give. So {@code @SolaceListener(concurrency)} sizing only the flow count would buy
 * nothing; this container also owns a handler pool sized from that same number, and that pool is
 * where the actual parallelism comes from. Two listeners no longer share one one pool, so a slow
 * handler on one cannot starve the other.
 *
 * <h2>Two acknowledgement modes</h2>
 * {@code CLIENT} (the default) is what the previous section describes: this container acks
 * explicitly, only after the reply is published and confirmed. {@code AUTO} hands
 * acknowledgement to JCSMP itself, which — verified against a live broker, not assumed from the
 * docs alone — sends the ack the instant the delivery callback returns, regardless of what that
 * callback actually did. For that ack to mean "the handler finished" rather than "the handler
 * was scheduled," an AUTO listener's handler cannot be handed off to this container's pool the
 * way CLIENT's is; it has to run inline, on the callback itself. See {@link #onRequest} for
 * where that split happens, and its cost: this callback runs on the one dispatch thread shared
 * by every flow in the session, so a slow AUTO handler blocks delivery to every other listener
 * and every reply in the process, not just its own queue, for as long as it runs.
 *
 * <p>Even inline, AUTO's ack only means the handler returned — if that handler's last act is
 * publishing a reply, AUTO acks once the publish is <em>started</em>, not once the broker
 * confirms it, which is the one guarantee CLIENT is built around. And it goes further than a
 * weaker guarantee: AUTO has no failure path at all. Settlement outcomes — what
 * {@link RetryableHandlerException} and a failed reply-publish both rely on to actively force a
 * redelivery — are a CLIENT-ack-only capability in JCSMP itself, confirmed against Solace's own
 * documentation, not assumed. Under AUTO, JCSMP has already decided to acknowledge the message
 * the moment the callback returns, and nothing the handler does can change that after the fact;
 * see {@link #settleFailed} for where this is made explicit rather than silently swallowed. AUTO
 * fits a fast handler that does not reply and can tolerate an occasional failure going unnoticed;
 * CLIENT is the right choice for everything else.
 *
 * <h2>Backpressure</h2>
 * {@code Executors.newFixedThreadPool} — what this container's handler pool used to be built
 * from unconditionally — backs onto an <em>unbounded</em> queue. A handler that cannot keep up
 * with broker delivery did not slow delivery down at all: every request kept landing in this
 * process's heap as a queued {@code Runnable} holding the raw message, with nothing to bound how
 * far that grew. For a {@code CLIENT}-ack listener (the only one whose handler runs on this
 * pool — see above), this container now bounds that queue via
 * {@code replier.backpressure.queue-capacity} and, once it reaches
 * {@code pause-at-queue-depth}, pauses this listener's own flow(s) via {@link FlowConsumer#pause}
 * so the backlog accumulates on the broker's durable queue instead — visible, monitored, and
 * already governed by {@code replier.provision.max-redelivery} and the DMQ — rather than
 * invisibly in this process's memory. Delivery resumes at {@code resume-at-queue-depth}. A
 * message that still arrives after the queue is genuinely full (the broker's in-flight window did
 * not drain in time to see the pause) is settled {@code FAILED} in {@link #onRejected} rather than
 * accepted unconditionally, so it redelivers instead of piling up regardless. {@code AUTO}-ack
 * listeners are unaffected: their handler already runs inline on the dispatch thread, which is
 * self-throttling by construction.
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
    private final boolean replyDmqEligible;
    /** Resolved once at construction, not per message: "unset" already means request.timeout. */
    private final long replyTtlMillis;
    private final SolaceRequestReplyProperties.Backpressure.Resolved backpressure;

    private final List<FlowConsumer> flows = new ArrayList<>();
    private final AtomicBoolean backpressureEngaged = new AtomicBoolean();
    private volatile Queue queue;
    private volatile boolean running;
    private volatile ThreadPoolExecutor handlerExecutor;

    public SolaceMessageListenerContainer(SolaceListenerEndpoint endpoint,
                                          SolaceSession session,
                                          RequestQueueProvisioner provisioner,
                                          PersistentPublisher publisher,
                                          PayloadCodec codec,
                                          HandlerMethodInvoker invoker,
                                          SolaceListenerErrorHandler errorHandler,
                                          boolean replyDmqEligible,
                                          long replyTtlMillis,
                                          SolaceRequestReplyProperties.Backpressure.Resolved backpressure) {
        this.endpoint = endpoint;
        this.session = session;
        this.provisioner = provisioner;
        this.publisher = publisher;
        this.codec = codec;
        this.invoker = invoker;
        this.errorHandler = errorHandler;
        this.replyDmqEligible = replyDmqEligible;
        this.replyTtlMillis = replyTtlMillis;
        this.backpressure = backpressure;
    }

    public synchronized void start() {
        if (running) { return; }
        queue = provisioner.ensure(endpoint.queue(), endpoint.topics());
        int n = Math.max(1, endpoint.concurrency());
        handlerExecutor = newHandlerExecutor(n);
        for (int i = 0; i < endpoint.concurrency(); i++) {
            FlowConsumer flow = new FlowConsumer(session, queue,
                    endpoint.id() + "-" + i, endpoint.clientAck(), this::onRequest);
            flow.start();
            flows.add(flow);
        }
        session.onReconnect(this::onReconnect);
        running = true;
        log.info("Listener '{}' started: queue={} concurrency={} topics={} backpressure={}",
                endpoint.id(), endpoint.queue(), endpoint.concurrency(), endpoint.topics(),
                endpoint.clientAck() && backpressure.enabled()
                        ? "capacity=" + backpressure.queueCapacity()
                                + " pauseAt=" + backpressure.pauseAtQueueDepth()
                                + " resumeAt=" + backpressure.resumeAtQueueDepth()
                        : "disabled");
    }

    /**
     * A {@code CLIENT}-ack listener with backpressure enabled gets a bounded queue and a handler
     * that settles a rejected message {@code FAILED} instead of buffering it unconditionally.
     * Everything else (backpressure disabled, or {@code AUTO}-ack, whose handler never touches
     * this pool at all — see {@link #onRequest}) keeps the original unbounded queue, matching
     * prior behaviour exactly.
     */
    private ThreadPoolExecutor newHandlerExecutor(int n) {
        ThreadFactory threadFactory = named("rr-handler-" + endpoint.id() + "-");
        if (!endpoint.clientAck() || !backpressure.enabled()) {
            return new ThreadPoolExecutor(n, n, 0L, TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<>(), threadFactory);
        }
        return new ThreadPoolExecutor(n, n, 0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(backpressure.queueCapacity()), threadFactory,
                this::onRejected);
    }

    private static ThreadFactory named(String prefix) {
        AtomicInteger n = new AtomicInteger();
        return r -> {
            Thread t = new Thread(r, prefix + n.incrementAndGet());
            t.setDaemon(true);
            return t;
        };
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
     * Dispatch. Runs on JCSMP's shared dispatch thread — shared across every flow in the
     * session, not one per flow.
     *
     * <p>CLIENT-ack listeners hand the handler to this container's own pool: business logic on
     * the dispatch thread would stall every other request on this instance, regardless of how
     * many flows are bound, and a nested request from it could never complete. AUTO-ack
     * listeners cannot take that path — JCSMP acks the moment this method returns, so the
     * handler has to have already run, inline, for that ack to correspond to anything real.
     */
    private void onRequest(BytesXMLMessage raw) {
        if (!endpoint.clientAck()) {
            handle(raw);
            return;
        }
        ThreadPoolExecutor pool = handlerExecutor;
        if (pool == null) {
            // Only reachable while shutting down: close() nulls this before the flow that
            // delivered raw has necessarily stopped. Not acknowledging is correct here — the
            // broker redelivers once this instance is gone rather than the message being lost.
            log.warn("Listener '{}' is closing; leaving correlationId={} unacknowledged so it is "
                    + "redelivered", endpoint.id(), raw.getCorrelationId());
            return;
        }
        pool.execute(new HandleTask(raw));
        // Checked after handing off, not before: the depth that matters for the pause decision
        // is the one left behind by this submission, and reading it any earlier would race the
        // increment execute() just performed.
        maybePause(pool);
    }

    /**
     * Wraps the unit of work so the rejection handler ({@link #onRejected}) can recover the
     * original message when the queue is full. A bare {@code () -> handle(raw)} lambda would
     * reach that handler as an opaque {@code Runnable} with no way back to {@code raw}.
     */
    private final class HandleTask implements Runnable {
        private final BytesXMLMessage raw;

        HandleTask(BytesXMLMessage raw) { this.raw = raw; }

        @Override
        public void run() {
            try {
                handle(raw);
            } finally {
                // Checked on the way out of every task, success or failure, so a backlog that
                // drains because handlers are keeping up (not because messages stopped arriving)
                // still resumes a paused flow promptly instead of only being noticed while a new
                // request happens to arrive.
                ThreadPoolExecutor pool = handlerExecutor;
                if (pool != null) { maybeResume(pool); }
            }
        }
    }

    /** Pauses this listener's flow(s) once the handoff queue reaches its pause threshold. */
    private void maybePause(ThreadPoolExecutor pool) {
        if (!endpoint.clientAck() || !backpressure.enabled()) { return; }
        int depth = pool.getQueue().size() + pool.getActiveCount();
        if (depth >= backpressure.pauseAtQueueDepth() && backpressureEngaged.compareAndSet(false, true)) {
            log.warn("Listener '{}' handler backlog reached {} (pause threshold {}); pausing flow "
                    + "delivery until it drains to {} rather than buffering further requests in "
                    + "memory", endpoint.id(), depth, backpressure.pauseAtQueueDepth(),
                    backpressure.resumeAtQueueDepth());
            flows.forEach(FlowConsumer::pause);
        }
    }

    /** Resumes this listener's flow(s) once the handoff queue drains to its resume threshold. */
    private void maybeResume(ThreadPoolExecutor pool) {
        if (!backpressureEngaged.get()) { return; }
        int depth = pool.getQueue().size() + pool.getActiveCount();
        if (depth <= backpressure.resumeAtQueueDepth() && backpressureEngaged.compareAndSet(true, false)) {
            log.info("Listener '{}' handler backlog drained to {}; resuming flow delivery",
                    endpoint.id(), depth);
            flows.forEach(FlowConsumer::resume);
        }
    }

    /**
     * A message that still arrived after the handoff queue was already full — the broker's
     * in-flight window did not drain in time to see {@link #maybePause} take effect — is settled
     * {@code FAILED} rather than accepted unconditionally. That forces a redelivery, subject to
     * {@code replier.provision.max-redelivery} and the DMQ, instead of this process buffering
     * unbounded work in memory regardless of the pool's own capacity.
     */
    private void onRejected(Runnable task, ThreadPoolExecutor pool) {
        if (task instanceof HandleTask t) {
            log.warn("Listener '{}' handler queue is full (capacity={}); settling "
                    + "correlationId={} FAILED so the broker redelivers it instead of this "
                    + "process buffering unbounded work in memory", endpoint.id(),
                    backpressure.queueCapacity(), t.raw.getCorrelationId());
            settleFailed(t.raw, t.raw.getCorrelationId());
        } else {
            log.warn("Listener '{}' handler queue is full; discarding an internal task",
                    endpoint.id());
        }
    }

    /**
     * Entry point for the actual work; {@link #process} does it. This wrapper exists only to
     * catch what {@code process} does not already handle itself — most plausibly
     * {@link InboundMessage#toModel} throwing on a malformed message, before {@code process}'s
     * own try/catch even starts. Letting that escape to {@link FlowConsumer}'s generic catch
     * would leave it there instead of here, where {@link #settleFailed} knows what an ack-mode
     * actually allows; routing it through the same call as every other failure keeps that one
     * true, instead of a message that quietly slips through with no active disposition at all.
     */
    private void handle(BytesXMLMessage raw) {
        try {
            process(raw);
        } catch (RuntimeException ex) {
            log.error("Unexpected failure handling correlationId={}; settling FAILED so the "
                    + "broker redelivers it, subject to replier.provision.max-redelivery",
                    raw.getCorrelationId(), ex);
            settleFailed(raw, raw.getCorrelationId());
        }
    }

    private void process(BytesXMLMessage raw) {
        RequestReplyMessage request = InboundMessage.toModel(raw);
        Object result = null;
        RequestReplyMessage reply;
        try {
            result = invoker.invoke(endpoint, request);
            reply = successReply(request, result);
        } catch (Exception ex) {
            Throwable cause = ex instanceof InvocationTargetException ite && ite.getCause() != null
                    ? ite.getCause() : ex;
            if (cause instanceof RetryableHandlerException rhe) {
                redeliver(raw, request, rhe);
                return;
            }
            reply = errorReply(request, cause);
        }

        if (reply == null) {
            acknowledge(raw, request.getCorrelationId());
            return;
        }

        String destination = endpoint.replyTo() != null && !endpoint.replyTo().isBlank()
                ? endpoint.replyTo() : request.getReplyTo();
        if (destination == null || destination.isBlank()) {
            log.warn("Request correlationId={} carries no reply-to and no @SendTo destination is "
                    + "configured; dropping the reply", request.getCorrelationId());
            acknowledge(raw, request.getCorrelationId());
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
                    acknowledge(raw, request.getCorrelationId());
                } else {
                    log.error("Reply for correlationId={} was not spooled; settling FAILED so "
                                    + "the broker redelivers the request, subject to "
                                    + "replier.provision.max-redelivery",
                            request.getCorrelationId(), err);
                    settleFailed(raw, request.getCorrelationId());
                }
            });
        } catch (RuntimeException ex) {
            log.error("Could not publish the reply for correlationId={}; settling FAILED so the "
                            + "broker redelivers the request, subject to "
                            + "replier.provision.max-redelivery",
                    request.getCorrelationId(), ex);
            settleFailed(raw, request.getCorrelationId());
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

    private RequestReplyMessage errorReply(RequestReplyMessage request, Throwable cause) {
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

    /**
     * Settles {@code raw} FAILED instead of publishing a reply, so the broker actively
     * redelivers it rather than the request merely sitting unacknowledged — for a CLIENT-ack
     * listener. See {@link #settleFailed} for why an AUTO-ack listener cannot make the same
     * promise, and {@link RetryableHandlerException} for the full contract and its caveats.
     */
    private void redeliver(BytesXMLMessage raw, RequestReplyMessage request,
                           RetryableHandlerException cause) {
        log.warn("Handler for correlationId={} threw a retryable failure ({}); settling FAILED "
                + "so the broker redelivers it, subject to replier.provision.max-redelivery",
                request.getCorrelationId(), cause.getMessage());
        settleFailed(raw, request.getCorrelationId());
    }

    /**
     * Actively requests redelivery, for CLIENT-ack listeners. Shared by every failure path that
     * ends up here — a retryable handler failure, a reply that could not be published, one whose
     * publish was rejected after the fact, or anything else {@link #process} did not already
     * turn into a reply — because none of them are fixed by merely not acknowledging {@code raw}:
     * the broker only reclaims an unacknowledged message on disconnect, not on demand. Settling
     * FAILED is what actually triggers a redelivery, and is what makes each caller's log line
     * true — for CLIENT.
     *
     * <p>For AUTO there is no equivalent call to make. Settlement outcomes — FAILED and REJECTED
     * alike — are a CLIENT-ack-only capability in JCSMP itself, confirmed against Solace's own
     * JCSMP acknowledgment guide and a live broker, not assumed: an AUTO-ack flow never declares
     * {@code addRequiredSettlementOutcomes}, and declaring it anyway does not help, because the
     * broker does not honor the outcome outside CLIENT ack. By the time any of this class's
     * failure paths run, JCSMP has either already acknowledged {@code raw} or is going to the
     * moment the callback returns, and nothing here can change that. So for AUTO this method logs
     * the failure honestly instead of claiming a redelivery that cannot happen.
     */
    private void settleFailed(BytesXMLMessage raw, String correlationId) {
        if (!endpoint.clientAck()) {
            log.error("CorrelationId={} cannot be actively redelivered under AUTO ack; "
                    + "settlement outcomes require CLIENT ack in JCSMP, so JCSMP has already "
                    + "acknowledged this message, or will the moment the handler returns, "
                    + "regardless of this failure", correlationId);
            return;
        }
        try {
            raw.settle(XMLMessage.Outcome.FAILED);
        } catch (JCSMPException e) {
            log.error("Could not settle correlationId={} as FAILED; leaving it unacknowledged "
                    + "instead, which redelivers only on the next reconnect", correlationId, e);
        }
    }

    private void acknowledge(BytesXMLMessage raw, String correlationId) {
        if (!endpoint.clientAck()) { return; }
        try {
            raw.ackMessage();
        } catch (IllegalStateException e) {
            // The one documented cause: the flow this message arrived on is already closed.
            // The broker will redeliver once it reconnects -- correct behaviour, silently. Every
            // other failure path in this class logs before it does anything else; this is the
            // one place that did not.
            log.warn("Could not acknowledge correlationId={}; the flow was already closed. The "
                    + "broker will redeliver once it reconnects", correlationId, e);
        }
    }

    public boolean isRunning() { return running; }

    public String id() { return endpoint.id(); }

    /** Requests queued plus running in the handler pool, or {@code 0} before {@link #start()}. */
    public int queueDepth() {
        ThreadPoolExecutor pool = handlerExecutor;
        return pool == null ? 0 : pool.getQueue().size() + pool.getActiveCount();
    }

    /** Whether this listener's flow(s) are currently paused for backpressure. */
    public boolean isBackpressureEngaged() { return backpressureEngaged.get(); }

    @Override
    public synchronized void close() {
        running = false;
        for (FlowConsumer f : flows) { f.close(); }
        flows.clear();
        // Null before shutdown: a concurrent onRequest reads this field, and it must never see a
        // pool that has already stopped accepting work.
        ThreadPoolExecutor pool = handlerExecutor;
        handlerExecutor = null;
        if (pool != null) { pool.shutdown(); }
    }
}
