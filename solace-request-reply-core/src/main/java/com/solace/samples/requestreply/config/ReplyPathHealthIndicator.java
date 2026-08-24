package com.solace.samples.requestreply.config;

import com.solace.samples.requestreply.core.DefaultReplyingSolaceTemplate;
import com.solace.samples.requestreply.endpoint.ReplyEndpoint;
import com.solace.samples.requestreply.transport.SolaceSession;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;

/**
 * Reports the reply path through Actuator, so an orchestrator can act on it.
 *
 * <p>This exists because the most damaging failure in this design is silent. A temporary reply
 * queue recreated after its linger window has no topic subscription: the session is connected, the
 * flow is bound, the queue exists, nothing is logged, and every request times out until the process
 * restarts. Reporting DOWN lets a readiness probe remove the instance instead of leaving it to
 * accept requests it can never answer.
 */
public class ReplyPathHealthIndicator implements HealthIndicator {

    private final SolaceSession session;
    private final ReplyEndpoint replyEndpoint;
    private final DefaultReplyingSolaceTemplate template;

    public ReplyPathHealthIndicator(SolaceSession session, ReplyEndpoint replyEndpoint,
                                    DefaultReplyingSolaceTemplate template) {
        this.session = session;
        this.replyEndpoint = replyEndpoint;
        this.template = template;
    }

    @Override
    public Health health() {
        boolean connected = session.isConnected();
        boolean established = replyEndpoint.isEstablished();
        boolean verified = template.isReplyPathVerified();

        Health.Builder builder = (connected && established && verified) ? Health.up() : Health.down();
        return builder
                .withDetail("sessionConnected", connected)
                .withDetail("lastSessionEvent", session.lastEvent())
                .withDetail("reconnects", session.reconnectCount())
                .withDetail("replyEndpointEstablished", established)
                .withDetail("replyQueue", established ? replyEndpoint.queue().getName() : null)
                .withDetail("replySubscription", replyEndpoint.subscription())
                .withDetail("replyPathVerified", verified)
                .withDetail("replyPathDetail", template.replyPathDetail())
                .build();
    }
}
