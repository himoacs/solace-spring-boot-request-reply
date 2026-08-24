package com.solace.samples.requestreply.endpoint;

import com.solace.samples.requestreply.config.SolaceRequestReplyProperties;
import com.solace.samples.requestreply.transport.SolaceSession;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

/** Builds the configured reply endpoint and resolves this instance's identity. */
public class ReplyEndpointFactory {

    private final SolaceSession session;
    private final SolaceRequestReplyProperties props;

    public ReplyEndpointFactory(SolaceSession session, SolaceRequestReplyProperties props) {
        this.session = session;
        this.props = props;
    }

    public ReplyEndpoint create() {
        SolaceRequestReplyProperties.Reply cfg = props.getReply();
        String instanceId = resolveInstanceId(cfg.getInstanceId());

        Map<String, String> statics = new LinkedHashMap<>(cfg.getPlaceholders());
        statics.put("instanceId", instanceId);

        ReplyTopicPattern pattern = new ReplyTopicPattern(
                cfg.getTopicPattern(), cfg.getPerRequestPlaceholders(), statics);

        String queueName = cfg.getQueueNamePattern().replace("{instanceId}", instanceId);

        return switch (cfg.getEndpointType()) {
            case DURABLE -> new DurableReplyEndpoint(session, pattern, queueName, cfg.getQuotaMb());
            case TEMPORARY -> new TemporaryReplyEndpoint(session, pattern, queueName);
        };
    }

    /**
     * Hostname plus a short random suffix when not configured.
     *
     * <p>The suffix matters for the temporary case: two processes on one host must not collide,
     * and a restarted process must not inherit replies addressed to its previous incarnation,
     * whose futures died with it.
     */
    static String resolveInstanceId(String configured) {
        if (configured != null && !configured.isBlank()) {
            return ReplyTopicPattern.sanitize(configured);
        }
        String host = System.getenv("HOSTNAME");
        if (host == null || host.isBlank()) {
            try {
                host = InetAddress.getLocalHost().getHostName();
            } catch (UnknownHostException e) {
                host = "unknown-host";
            }
        }
        return ReplyTopicPattern.sanitize(host) + "-" + UUID.randomUUID().toString().substring(0, 8);
    }
}
