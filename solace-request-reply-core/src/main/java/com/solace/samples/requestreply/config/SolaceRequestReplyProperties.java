package com.solace.samples.requestreply.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Configuration under {@code solace.request-reply.*}.
 *
 * <p>Connection settings deliberately live elsewhere, under {@code solace.java.*}, which is
 * the namespace the official {@code solace-java-spring-boot-starter} already binds. Nothing
 * here duplicates them.
 */
@ConfigurationProperties(prefix = "solace.request-reply")
public class SolaceRequestReplyProperties {

    private boolean enabled = true;

    @NestedConfigurationProperty
    private final Request request = new Request();
    @NestedConfigurationProperty
    private final Reply reply = new Reply();
    @NestedConfigurationProperty
    private final Replier replier = new Replier();
    @NestedConfigurationProperty
    private final Dmq dmq = new Dmq();

    public boolean isEnabled() { return enabled; }
    public void setEnabled(boolean enabled) { this.enabled = enabled; }
    public Request getRequest() { return request; }
    public Reply getReply() { return reply; }
    public Replier getReplier() { return replier; }
    public Dmq getDmq() { return dmq; }

    // ------------------------------------------------------------------ request

    public static class Request {
        /** Default reply deadline when a caller does not pass one. */
        private Duration timeout = Duration.ofSeconds(5);
        /**
         * Set message TTL equal to {@link #timeout}. Strongly recommended: without it a
         * request can outlive the requestor's patience and be processed by a replier after
         * nobody is waiting, producing a reservation the customer never sees confirmed.
         */
        private boolean ttlMatchesTimeout = true;
        /** Stamp a monotonic sequence number, enabling gap and reorder detection. */
        private boolean sequenceNumbers = true;
        /**
         * Mark published requests DMQ-eligible, so a request that exhausts its redeliveries or
         * outlives its TTL is moved to the dead message queue instead of being discarded.
         *
         * <p>Needed for brokers at 10.25.9 and earlier, where only eligible messages are moved.
         * From 10.25.10 the broker moves everything unless the queue sets
         * {@code respectDmqEligibleEnabled}, so setting the flag is the behaviour that is
         * correct on both. It does nothing at all unless the DMQ exists — see {@link Dmq}.
         */
        private boolean dmqEligible = true;
        /**
         * Caps concurrent in-flight {@code sendAndReceive} calls on this instance. {@code 0}
         * (the default) is unbounded, matching prior behaviour.
         *
         * <p>Without a cap, a traffic burst or a stalled replier grows the correlation store to
         * roughly {@code arrivalRate * timeout} entries before the timeout reaper catches up —
         * accepted silently, with no signal to the caller that it should slow down. Setting this
         * turns that into a fast, typed {@code RequestBackpressureException} the moment the cap
         * is hit, instead of an ever-growing map of futures waiting on a reply that may never
         * come.
         */
        private int maxPending = 0;
        /**
         * How often {@link com.solace.samples.requestreply.core.TimeoutReaper} sweeps the
         * correlation store for expired requests. Lower values fail expired requests sooner, at
         * the cost of copying the store's pending entries more often.
         */
        private Duration reaperSweepInterval = Duration.ofMillis(100);
        /**
         * How stale the reaper's last successful sweep may get before the reply path reports
         * itself unhealthy. Should be a small multiple of {@link #reaperSweepInterval}.
         *
         * <p>The reaper runs on a {@code scheduleWithFixedDelay} task, whose documented
         * behaviour is to stop forever, silently, the first time the task throws. From that
         * moment every future request leaks its {@code CompletableFuture} and its correlation
         * store entry for the life of the process, because nothing else ever evicts it. This
         * threshold is what turns that silent failure into an observable one.
         */
        private Duration reaperMaxStaleness = Duration.ofSeconds(5);

        public Duration getTimeout() { return timeout; }
        public void setTimeout(Duration v) { this.timeout = v; }
        public boolean isTtlMatchesTimeout() { return ttlMatchesTimeout; }
        public void setTtlMatchesTimeout(boolean v) { this.ttlMatchesTimeout = v; }
        public boolean isSequenceNumbers() { return sequenceNumbers; }
        public void setSequenceNumbers(boolean v) { this.sequenceNumbers = v; }
        public boolean isDmqEligible() { return dmqEligible; }
        public void setDmqEligible(boolean v) { this.dmqEligible = v; }
        public int getMaxPending() { return maxPending; }
        public void setMaxPending(int v) { this.maxPending = v; }
        public Duration getReaperSweepInterval() { return reaperSweepInterval; }
        public void setReaperSweepInterval(Duration v) { this.reaperSweepInterval = v; }
        public Duration getReaperMaxStaleness() { return reaperMaxStaleness; }
        public void setReaperMaxStaleness(Duration v) { this.reaperMaxStaleness = v; }
    }

    // -------------------------------------------------------------------- reply

    public static class Reply {
        /**
         * Whether this process needs a reply endpoint at all. True for anything that sends
         * requests; <b>false for a replier-only process</b>.
         *
         * <p>A replier consumes the shared request queue and publishes to each request's
         * {@code replyTo} topic. Nothing is ever addressed to a reply queue of its own, so
         * leaving this on provisions a durable, exclusive queue that is subscribed, bound, and
         * then receives nothing for its entire life.
         *
         * <p>That is not merely waste. The queue is named after the instance, so under a
         * Kubernetes Deployment — where pod names are regenerated on every rollout — each
         * rollout strands the previous pods' queues on the broker, one per pod, indefinitely.
         *
         * <p>Turning it off removes the reply endpoint, the requestor-side template and the
         * reply-path health indicator. Anything that injects {@code ReplyingSolaceTemplate}
         * must tolerate its absence.
         */
        private boolean enabled = true;

        /**
         * Reply topic pattern. {@code {placeholders}} are substituted per request; those
         * named in {@link #perRequestPlaceholders} become {@code *} in the subscription and
         * are filled at publish time.
         */
        private String topicPattern = "requestreply/reply/v1/{instanceId}";

        /** Placeholders resolved per request rather than once at startup. */
        private List<String> perRequestPlaceholders = new ArrayList<>();

        /**
         * SpEL per per-request placeholder, evaluated against the request payload.
         *
         * <p>Without an expression a placeholder has no value and the level renders as
         * {@code unknown} — the subscription still matches, because that level is wildcarded, so
         * the failure is silent and costs only observability. Naming the expression is what makes
         * the train number actually appear in the reply topic.
         */
        private java.util.Map<String, String> perRequestPlaceholderExpressions =
                new java.util.LinkedHashMap<>();

        /** Static placeholder values, e.g. {@code zone: nr}. */
        private java.util.Map<String, String> placeholders = new java.util.LinkedHashMap<>();

        /**
         * Instance identity, substituted into the reply topic and queue name. Blank resolves to
         * the hostname, which is the pod name on Kubernetes.
         *
         * <p>Must be unique per instance and stable across restarts. The reply queue is durable
         * and exclusive, so two instances sharing an id bind the same queue and the second
         * silently receives nothing, while an id that changes between runs strands the previous
         * queue on the broker. Set this explicitly when running several instances on one host.
         */
        private String instanceId = "";

        /** Queue name pattern; {@code {instanceId}} is substituted. */
        private String queueNamePattern = "q.requestreply.reply.{instanceId}";



        /** How long {@code waitForReplyEndpoint} blocks at startup. */
        private Duration waitForEndpoint = Duration.ofSeconds(10);

        private int quotaMb = 100;

        /**
         * How the reply queue is provisioned. Mirrors {@code replier.provision.mode} — same
         * enum, same meaning: {@code CREATE_IF_MISSING} (the default) creates it if absent and
         * fails on drift; {@code OFF} assumes it exists and only subscribes.
         *
         * <p>On a message VPN whose client profile forbids creating endpoints, the replier
         * already has this escape hatch via {@code replier.provision.mode: OFF}; the requestor
         * did not, and simply failed to start. Since the reply queue is named per instance
         * rather than shared, using {@code OFF} here means provisioning one queue per instance
         * identity out of band in advance — practical when identities are known ahead of time,
         * such as a StatefulSet's stable ordinals, more work when they are not.
         */
        private ProvisionMode provisionMode = ProvisionMode.CREATE_IF_MISSING;

        public boolean isEnabled() { return enabled; }
        public void setEnabled(boolean v) { this.enabled = v; }
        public String getTopicPattern() { return topicPattern; }
        public void setTopicPattern(String v) { this.topicPattern = v; }
        public List<String> getPerRequestPlaceholders() { return perRequestPlaceholders; }
        public void setPerRequestPlaceholders(List<String> v) { this.perRequestPlaceholders = v; }
        public java.util.Map<String, String> getPerRequestPlaceholderExpressions() {
            return perRequestPlaceholderExpressions;
        }
        public void setPerRequestPlaceholderExpressions(java.util.Map<String, String> v) {
            this.perRequestPlaceholderExpressions = v;
        }
        public java.util.Map<String, String> getPlaceholders() { return placeholders; }
        public void setPlaceholders(java.util.Map<String, String> v) { this.placeholders = v; }
        public String getInstanceId() { return instanceId; }
        public void setInstanceId(String v) { this.instanceId = v; }
        public String getQueueNamePattern() { return queueNamePattern; }
        public void setQueueNamePattern(String v) { this.queueNamePattern = v; }
        public Duration getWaitForEndpoint() { return waitForEndpoint; }
        public void setWaitForEndpoint(Duration v) { this.waitForEndpoint = v; }
        public int getQuotaMb() { return quotaMb; }
        public void setQuotaMb(int v) { this.quotaMb = v; }
        public ProvisionMode getProvisionMode() { return provisionMode; }
        public void setProvisionMode(ProvisionMode v) { this.provisionMode = v; }
    }

    // ------------------------------------------------------------------ replier

    /**
     * Two modes, not three. A mode that validates configuration against the broker <em>without
     * ever creating</em> looks appealing, but JCSMP has no such call: {@code provision()}
     * creates a missing endpoint unconditionally, regardless of any flag — verified against a
     * live broker, see {@code spike/README.md}. The only way to offer that third mode honestly
     * would be to probe existence some other way first (an existence-only bind, say) before
     * ever calling {@code provision()}, and that is machinery this library does not carry for a
     * mode whose entire appeal was supposed to be doing less, not more.
     */
    public enum ProvisionMode {
        /** Assume the queue exists. For environments under change management. */
        OFF,
        /**
         * Provision if absent, fail on drift. The default; makes a fresh broker just work.
         * Drift detection needs no SEMP: JCSMP raises {@code PropertyMismatchException}
         * carrying the offending property name, whether or not the queue had to be created.
         */
        CREATE_IF_MISSING
    }

    public enum AccessType {
        /** One flow may bind at a time. */
        EXCLUSIVE,
        /** Several flows may bind and compete for messages. The default, and what scale-out needs. */
        NON_EXCLUSIVE
    }

    public static class Replier {
        private String queue;
        private List<String> topics = new ArrayList<>();
        /** Non-exclusive is what gives competing consumers. */
        private AccessType accessType = AccessType.NON_EXCLUSIVE;
        /**
         * Default for {@code @SolaceListener(concurrency = "${solace.request-reply.replier.concurrency}")}
         * when a listener reads it this way, as the demo's does. The library itself never reads
         * this value directly — each listener's own attribute is what actually sizes its flows
         * and its handler pool.
         */
        private int concurrency = 4;

        /** Mark published replies DMQ-eligible. See {@link Request#isDmqEligible()}. */
        private boolean dmqEligible = true;
        /**
         * TTL on published replies. <b>Unset means follow {@code request.timeout}</b>, which is
         * the default; {@code 0s} disables expiry.
         *
         * <p>A reply is only useful to the one requestor instance whose future is waiting, and
         * that future has a deadline — past it, no process can complete it. Without a TTL an
         * undeliverable reply sits in the reply queue indefinitely, which is the orphaned-queue
         * accumulation {@code DurableReplyEndpoint} warns about.
         *
         * <p>It derives from the request timeout rather than defaulting to a fixed duration on
         * purpose: a hard-coded value would expire replies while requestors were still waiting
         * as soon as anyone raised {@code request.timeout}.
         */
        private Duration replyTtl;

        @NestedConfigurationProperty
        private final Provision provision = new Provision();
        @NestedConfigurationProperty
        private final Backpressure backpressure = new Backpressure();

        public String getQueue() { return queue; }
        public void setQueue(String v) { this.queue = v; }
        public List<String> getTopics() { return topics; }
        public void setTopics(List<String> v) { this.topics = v; }
        public AccessType getAccessType() { return accessType; }
        public void setAccessType(AccessType v) { this.accessType = v; }
        public int getConcurrency() { return concurrency; }
        public void setConcurrency(int v) { this.concurrency = v; }
        public Provision getProvision() { return provision; }
        public boolean isDmqEligible() { return dmqEligible; }
        public void setDmqEligible(boolean v) { this.dmqEligible = v; }
        public Duration getReplyTtl() { return replyTtl; }
        public void setReplyTtl(Duration v) { this.replyTtl = v; }
        public Backpressure getBackpressure() { return backpressure; }

        /** Reply TTL in millis, resolving "unset" against the request timeout. */
        public long resolveReplyTtlMillis(Duration requestTimeout) {
            Duration d = replyTtl != null ? replyTtl : requestTimeout;
            return d == null || d.isNegative() ? 0L : d.toMillis();
        }
    }

    /**
     * Bounds the in-process handoff between a {@code CLIENT}-ack listener's flow(s) and its
     * handler pool.
     *
     * <p>{@code Executors.newFixedThreadPool} backs onto an <em>unbounded</em> queue. Left alone,
     * a handler that cannot keep up with broker delivery does not slow the broker down at all —
     * every request keeps landing in this process's heap as a queued {@code Runnable} holding the
     * raw message, growing without limit. This bounds that queue and, once it fills to
     * {@link #pauseAtQueueDepth}, pauses the listener's own flow(s) so the backlog accumulates on
     * the broker's durable queue instead — visible, monitored, and already governed by
     * {@code replier.provision.max-redelivery} and the DMQ — rather than invisibly in this
     * process's memory. Delivery resumes once the backlog drains to {@link #resumeAtQueueDepth}.
     *
     * <p>A message that arrives after the queue is already full (the broker's in-flight window
     * did not drain in time to see the pause) is settled {@code FAILED} rather than accepted
     * unconditionally, so it redelivers instead of piling up regardless.
     */
    public static class Backpressure {
        private boolean enabled = true;
        /** {@code <= 0} resolves to {@code 2 * concurrency} for that listener. */
        private int queueCapacity = 0;
        /** {@code <= 0} resolves to {@link #queueCapacity} — pause once the buffer is full. */
        private int pauseAtQueueDepth = 0;
        /** {@code <= 0} resolves to half of the resolved {@link #queueCapacity}. */
        private int resumeAtQueueDepth = 0;

        public boolean isEnabled() { return enabled; }
        public void setEnabled(boolean v) { this.enabled = v; }
        public int getQueueCapacity() { return queueCapacity; }
        public void setQueueCapacity(int v) { this.queueCapacity = v; }
        public int getPauseAtQueueDepth() { return pauseAtQueueDepth; }
        public void setPauseAtQueueDepth(int v) { this.pauseAtQueueDepth = v; }
        public int getResumeAtQueueDepth() { return resumeAtQueueDepth; }
        public void setResumeAtQueueDepth(int v) { this.resumeAtQueueDepth = v; }

        /** Resolves every {@code <= 0} ("auto") value against one listener's own concurrency. */
        public Resolved resolve(int concurrency) {
            int capacity = queueCapacity > 0 ? queueCapacity : Math.max(1, concurrency * 2);
            int pauseAt = pauseAtQueueDepth > 0 ? Math.min(pauseAtQueueDepth, capacity) : capacity;
            int resumeAt = resumeAtQueueDepth > 0
                    ? Math.min(resumeAtQueueDepth, pauseAt)
                    : Math.max(1, capacity / 2);
            return new Resolved(enabled, capacity, pauseAt, resumeAt);
        }

        /** Fully resolved settings for one listener; no more {@code <= 0} "auto" values. */
        public record Resolved(boolean enabled, int queueCapacity, int pauseAtQueueDepth,
                               int resumeAtQueueDepth) {
        }
    }

    public static class Provision {
        private ProvisionMode mode = ProvisionMode.CREATE_IF_MISSING;
        private int quotaMb = 5000;
        /** 0 means redeliver forever, which lets one poison message loop indefinitely. */
        private int maxRedelivery = 3;
        private boolean respectsTtl = true;
        /** Lets the requestor learn its request was discarded rather than just timing out. */
        private boolean discardNotifySender = true;

        public ProvisionMode getMode() { return mode; }
        public void setMode(ProvisionMode v) { this.mode = v; }
        public int getQuotaMb() { return quotaMb; }
        public void setQuotaMb(int v) { this.quotaMb = v; }
        public int getMaxRedelivery() { return maxRedelivery; }
        public void setMaxRedelivery(int v) { this.maxRedelivery = v; }
        public boolean isRespectsTtl() { return respectsTtl; }
        public void setRespectsTtl(boolean v) { this.respectsTtl = v; }
        public boolean isDiscardNotifySender() { return discardNotifySender; }
        public void setDiscardNotifySender(boolean v) { this.discardNotifySender = v; }
    }

    /**
     * The dead message queue: where the broker puts a message it has given up on, instead of
     * deleting it.
     *
     * <p>A message lands here when it exhausts {@code replier.provision.max-redelivery} or its
     * TTL expires. Both halves have to be in place — the message marked eligible and the queue
     * existing — or the broker deletes it. <b>If the DMQ does not exist the message is deleted
     * even when it is eligible</b>, which is why this provisions it.
     *
     * <p>One shared queue rather than a DMQ per endpoint. Every queue already points at
     * {@code #DEAD_MSG_QUEUE} by default, so this needs no management access; naming a different
     * DMQ per endpoint would mean setting {@code deadMsgQueue} on each source queue over SEMP.
     * Dead-lettered messages keep their original topic, so requests and replies remain easy to
     * tell apart on inspection.
     */
    public static class Dmq {
        /** Mark messages eligible and provision the queue. */
        private boolean enabled = true;
        /**
         * The queue to provision and report as this application's DMQ.
         *
         * <p>{@code #DEAD_MSG_QUEUE} is the Message VPN default, and every queue's
         * {@code deadMsgQueue} already points at it, so with this default the queue named here is
         * genuinely the one messages arrive in.
         *
         * <p><b>Naming anything else provisions that queue but does not route to it.</b> Which
         * queue a message dead-letters into is {@code deadMsgQueue} on the source queue, and
         * {@code EndpointProperties} has no setter for it, so no JCSMP client can set it. Set it
         * over SEMP, the CLI or your configuration-management tool, on the request queue and on
         * every reply queue. A non-default name is logged as a warning for exactly this reason.
         */
        private String name = "#DEAD_MSG_QUEUE";
        /** Create the queue at startup when missing. Turn off if provisioned out of band. */
        private boolean provision = true;
        private int quotaMb = 1000;

        public boolean isEnabled() { return enabled; }
        public void setEnabled(boolean v) { this.enabled = v; }
        public String getName() { return name; }
        public void setName(String v) { this.name = v; }
        public boolean isProvision() { return provision; }
        public void setProvision(boolean v) { this.provision = v; }
        public int getQuotaMb() { return quotaMb; }
        public void setQuotaMb(int v) { this.quotaMb = v; }
    }

}
