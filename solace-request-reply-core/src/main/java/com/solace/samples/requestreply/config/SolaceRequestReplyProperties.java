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
    private final TracingProperties tracing = new TracingProperties();

    public boolean isEnabled() { return enabled; }
    public void setEnabled(boolean enabled) { this.enabled = enabled; }
    public Request getRequest() { return request; }
    public Reply getReply() { return reply; }
    public Replier getReplier() { return replier; }
    public TracingProperties getTracing() { return tracing; }

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
        /**
         * SpEL over the payload producing the partition key. Overridden by an explicit
         * argument to {@code sendAndReceive}.
         */
        private String partitionKeyExpression;
        /** Stamp a monotonic sequence number, enabling gap and reorder detection. */
        private boolean sequenceNumbers = true;

        public Duration getTimeout() { return timeout; }
        public void setTimeout(Duration v) { this.timeout = v; }
        public String getDeliveryMode() { return deliveryMode; }
        public void setDeliveryMode(String v) { this.deliveryMode = v; }
        public boolean isTtlMatchesTimeout() { return ttlMatchesTimeout; }
        public void setTtlMatchesTimeout(boolean v) { this.ttlMatchesTimeout = v; }
        public String getPartitionKeyExpression() { return partitionKeyExpression; }
        public void setPartitionKeyExpression(String v) { this.partitionKeyExpression = v; }
        public boolean isSequenceNumbers() { return sequenceNumbers; }
        public void setSequenceNumbers(boolean v) { this.sequenceNumbers = v; }
    }

    // -------------------------------------------------------------------- reply

    public enum ReplyEndpointType {
        /**
         * Broker-generated, auto-deleted. No provisioning and nothing to clean up, which is
         * why it is the default. Costs one hazard: the queue lingers only 60s after a
         * disconnect (180s across an HA failover) and is then recreated <em>without</em> its
         * subscription, so {@code recreate-on-reconnect} and the canary are load-bearing here.
         */
        TEMPORARY,
        /**
         * Provisioned and durable. The subscription is a broker-side object and survives
         * reconnects outright. Recommended for production, at the cost of needing a stable
         * instance identity and an orphan-drain policy.
         */
        DURABLE
    }

    public static class Reply {
        private ReplyEndpointType endpointType = ReplyEndpointType.TEMPORARY;

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

        /** Instance identity. Blank resolves to hostname plus a short random suffix. */
        private String instanceId = "";

        /** Queue name pattern; {@code {instanceId}} is substituted. */
        private String queueNamePattern = "q.requestreply.reply.{instanceId}";

        /** Re-provision, re-subscribe and re-bind after every reconnect. */
        private boolean recreateOnReconnect = true;

        /**
         * After reconnect, publish a probe to our own reply topic and require it back.
         * Turns the temporary-queue silent-death mode into a detectable failure.
         */
        private boolean canaryOnReconnect = true;
        private Duration canaryTimeout = Duration.ofSeconds(10);

        /** How long {@code waitForReplyEndpoint} blocks at startup. */
        private Duration waitForEndpoint = Duration.ofSeconds(10);

        private int quotaMb = 100;

        public ReplyEndpointType getEndpointType() { return endpointType; }
        public void setEndpointType(ReplyEndpointType v) { this.endpointType = v; }
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
        public boolean isRecreateOnReconnect() { return recreateOnReconnect; }
        public void setRecreateOnReconnect(boolean v) { this.recreateOnReconnect = v; }
        public boolean isCanaryOnReconnect() { return canaryOnReconnect; }
        public void setCanaryOnReconnect(boolean v) { this.canaryOnReconnect = v; }
        public Duration getCanaryTimeout() { return canaryTimeout; }
        public void setCanaryTimeout(Duration v) { this.canaryTimeout = v; }
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

        @NestedConfigurationProperty
        private final Provision provision = new Provision();
        @NestedConfigurationProperty
        private final Partitioning partitioning = new Partitioning();

        public String getQueue() { return queue; }
        public void setQueue(String v) { this.queue = v; }
        public List<String> getTopics() { return topics; }
        public void setTopics(List<String> v) { this.topics = v; }
        public String getAccessType() { return accessType; }
        public void setAccessType(String v) { this.accessType = v; }
        public int getConcurrency() { return concurrency; }
        public void setConcurrency(int v) { this.concurrency = v; }
        public Provision getProvision() { return provision; }
        public Partitioning getPartitioning() { return partitioning; }
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

    public static class Partitioning {
        /**
         * 0 is a flat queue. Above 0 expects a partitioned queue, which JCSMP cannot
         * provision — {@code EndpointProperties} has no partition member at any version — so
         * it is created or verified over SEMP.
         *
         * <p>Sizing, per Solace's guidance: the maximum number of consumers you expect to
         * bind. Concurrency 10 across 3 pods is 30 flows, so 32.
         */
        private int partitionCount = 0;
        private Duration rebalanceDelay = Duration.ofSeconds(5);
        private Duration rebalanceMaxHandoffTime = Duration.ofSeconds(3);
        /** Permit a destructive partition-count change. Off, because decreasing deletes messages. */
        private boolean allowPartitionResize = false;

        @NestedConfigurationProperty
        private final Semp semp = new Semp();

        public int getPartitionCount() { return partitionCount; }
        public void setPartitionCount(int v) { this.partitionCount = v; }
        public Duration getRebalanceDelay() { return rebalanceDelay; }
        public void setRebalanceDelay(Duration v) { this.rebalanceDelay = v; }
        public Duration getRebalanceMaxHandoffTime() { return rebalanceMaxHandoffTime; }
        public void setRebalanceMaxHandoffTime(Duration v) { this.rebalanceMaxHandoffTime = v; }
        public boolean isAllowPartitionResize() { return allowPartitionResize; }
        public void setAllowPartitionResize(boolean v) { this.allowPartitionResize = v; }
        public Semp getSemp() { return semp; }
    }

    /**
     * SEMP access, consulted only when {@code partitionCount > 0}.
     *
     * <p>These are <b>management</b> credentials, not the messaging ones. In production this
     * bootstrap belongs in an init container or Terraform so the running application never
     * holds them and can run with {@code VALIDATE} alone.
     */
    public static class Semp {
        private String url;
        private String username;
        private String password;
        private String msgVpn = "default";

        public String getUrl() { return url; }
        public void setUrl(String v) { this.url = v; }
        public String getUsername() { return username; }
        public void setUsername(String v) { this.username = v; }
        public String getPassword() { return password; }
        public void setPassword(String v) { this.password = v; }
        public String getMsgVpn() { return msgVpn; }
        public void setMsgVpn(String v) { this.msgVpn = v; }
    }
}
