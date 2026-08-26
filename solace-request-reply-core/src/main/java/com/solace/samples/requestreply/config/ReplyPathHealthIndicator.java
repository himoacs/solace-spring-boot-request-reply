package com.solace.samples.requestreply.config;

import com.solace.samples.requestreply.endpoint.ReplyEndpoint;
import com.solace.samples.requestreply.transport.SolaceSession;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;

/**
 * Reports the reply path through Actuator, so an orchestrator can act on it.
 *
 * <p>An instance whose session is down, or whose reply endpoint never came up, cannot answer a
 * request it accepts. Reporting DOWN lets a readiness probe take it out of rotation rather than
 * leaving it to collect requests it will only time out.
 *
 * <h2>Readiness, not liveness</h2>
 * This goes DOWN while the session is {@code RECONNECTING}, so wire it to a <b>readiness</b>
 * probe. On a <b>liveness</b> probe, Kubernetes would kill the pod during the very reconnect the
 * library's retry budget exists to survive, turning a transient network blip into a restart
 * storm. See also {@link SolaceSessionHealthIndicator}, which is registered even when this one is
 * not — this class exists only while {@code reply.enabled=true}.
 */
public class ReplyPathHealthIndicator implements HealthIndicator {

    private final SolaceSession session;
    private final ReplyEndpoint replyEndpoint;

    public ReplyPathHealthIndicator(SolaceSession session, ReplyEndpoint replyEndpoint) {
        this.session = session;
        this.replyEndpoint = replyEndpoint;
    }

    @Override
    public Health health() {
        boolean connected = session.isConnected();
        boolean established = replyEndpoint.isEstablished();
        Health.Builder builder = (connected && established) ? Health.up() : Health.down();
        return builder
                .withDetail("sessionConnected", connected)
                .withDetail("lastSessionEvent", session.lastEvent())
                .withDetail("reconnects", session.reconnectCount())
                .withDetail("replyEndpointEstablished", established)
                .withDetail("replyQueue", established ? replyEndpoint.queue().getName() : null)
                .withDetail("replySubscription", replyEndpoint.subscription())
                .build();
    }
}
