package com.solace.samples.requestreply.endpoint;

import com.solace.samples.requestreply.config.SolaceRequestReplyProperties;
import com.solace.samples.requestreply.transport.SolaceSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.LinkedHashMap;
import java.util.Map;

/** Builds this instance's reply endpoint and resolves its identity. */
public class ReplyEndpointFactory {

    private static final Logger log = LoggerFactory.getLogger(ReplyEndpointFactory.class);

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

        // Logged because a collision is otherwise silent and expensive: two processes resolving
        // the same id bind the same exclusive queue, the second receives nothing, and every one
        // of its requests times out with no error anywhere.
        log.info("Reply endpoint identity: instanceId={} queue={}", instanceId, queueName);

        return new DurableReplyEndpoint(session, pattern, queueName, cfg.getQuotaMb());
    }

    /**
     * The configured id, or the hostname.
     *
     * <p>No random suffix: the queue is durable, so a value that changed between runs would
     * strand the previous queue on the broker, still spooling replies nobody will read. The
     * hostname is stable across a restart and is the pod name on Kubernetes.
     *
     * <p>The corollary is that two instances sharing a host must be told apart explicitly, via
     * {@code solace.request-reply.reply.instance-id}.
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
        return ReplyTopicPattern.sanitize(host);
    }
}
