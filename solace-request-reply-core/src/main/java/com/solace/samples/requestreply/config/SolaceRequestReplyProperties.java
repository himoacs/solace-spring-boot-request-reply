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
        /** PERSISTENT or DIRECT. PERSISTENT is the point of this design. */
        private String deliveryMode = "PERSISTENT";
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

        public Duration getTimeout() { return timeout; }
        public void setTimeout(Duration v) { this.timeout = v; }
        public String getDeliveryMode() { return deliveryMode; }
        public void setDeliveryMode(String v) { this.deliveryMode = v; }
        public boolean isTtlMatchesTimeout() { return ttlMatchesTimeout; }
        public void setTtlMatchesTimeout(boolean v) { this.ttlMatchesTimeout = v; }
        public boolean isSequenceNumbers() { return sequenceNumbers; }
        public void setSequenceNumbers(boolean v) { this.sequenceNumbers = v; }
        public boolean isDmqEligible() { return dmqEligible; }
        public void setDmqEligible(boolean v) { this.dmqEligible = v; }
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
    }

    // ------------------------------------------------------------------ replier

    public enum ProvisionMode {
        /** Assume the queue exists. For environments under change management. */
        OFF,
        /**
         * Attempt provision and fail startup on drift, reporting which property differs.
         * Needs no SEMP: JCSMP raises {@code PropertyMismatchException} carrying the
         * offending property name.
         */
        VALIDATE,
        /** Provision if absent, fail on drift. The default; makes a fresh broker just work. */
        CREATE_IF_MISSING
    }

    public static class Replier {
        private String queue;
        private List<String> topics = new ArrayList<>();
        /** EXCLUSIVE or NON_EXCLUSIVE. Non-exclusive is what gives competing consumers. */
        private String accessType = "NON_EXCLUSIVE";
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

        public String getQueue() { return queue; }
        public void setQueue(String v) { this.queue = v; }
        public List<String> getTopics() { return topics; }
        public void setTopics(List<String> v) { this.topics = v; }
        public String getAccessType() { return accessType; }
        public void setAccessType(String v) { this.accessType = v; }
        public int getConcurrency() { return concurrency; }
        public void setConcurrency(int v) { this.concurrency = v; }
        public Provision getProvision() { return provision; }
        public boolean isDmqEligible() { return dmqEligible; }
        public void setDmqEligible(boolean v) { this.dmqEligible = v; }
        public Duration getReplyTtl() { return replyTtl; }
        public void setReplyTtl(Duration v) { this.replyTtl = v; }

        /** Reply TTL in millis, resolving "unset" against the request timeout. */
        public long resolveReplyTtlMillis(Duration requestTimeout) {
            Duration d = replyTtl != null ? replyTtl : requestTimeout;
            return d == null || d.isNegative() ? 0L : d.toMillis();
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
         * The queue every endpoint dead-letters into. {@code #DEAD_MSG_QUEUE} is the Message
         * VPN default, which every queue points at without further configuration. Naming
         * anything else additionally requires setting {@code deadMsgQueue} on each source queue
         * over SEMP, which this library does not do.
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
