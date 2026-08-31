package com.solace.samples.requestreply.config;

import com.solace.samples.requestreply.api.ReplyingSolaceTemplate;
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
 *
 * <h2>The reaper's own heartbeat</h2>
 * Also goes DOWN once {@link ReplyingSolaceTemplate#reaperLastSweepAgeMillis()} exceeds
 * {@code request.reaper-max-staleness}. A reaper that has stopped sweeping is a correlation store
 * that no longer bounds itself — every unanswered request from that point leaks its future and
 * its map entry for the rest of the process's life — and that is exactly the condition an
 * orchestrator should treat like any other failed dependency, not something left to a memory
 * profile to eventually reveal.
 */
public class ReplyPathHealthIndicator implements HealthIndicator {

    private final SolaceSession session;
    private final ReplyEndpoint replyEndpoint;
    private final ReplyingSolaceTemplate template;
    private final long reaperMaxStalenessMs;

    public ReplyPathHealthIndicator(SolaceSession session, ReplyEndpoint replyEndpoint,
                                    ReplyingSolaceTemplate template, long reaperMaxStalenessMs) {
        this.session = session;
        this.replyEndpoint = replyEndpoint;
        this.template = template;
        this.reaperMaxStalenessMs = reaperMaxStalenessMs;
    }

    @Override
    public Health health() {
        boolean connected = session.isConnected();
        boolean established = replyEndpoint.isEstablished();
        long reaperAgeMs = template.reaperLastSweepAgeMillis();
        // Negative means "unknown / not applicable" (e.g. a custom ReplyingSolaceTemplate), not
        // a failure -- only an age this implementation actually reports can ever mark it stale.
        boolean reaperHealthy = reaperAgeMs < 0 || reaperAgeMs <= reaperMaxStalenessMs;
        Health.Builder builder = (connected && established && reaperHealthy) ? Health.up() : Health.down();
        return builder
                .withDetail("sessionConnected", connected)
                .withDetail("lastSessionEvent", session.lastEvent())
                .withDetail("reconnects", session.reconnectCount())
                .withDetail("replyEndpointEstablished", established)
                .withDetail("replyQueue", established ? replyEndpoint.queue().getName() : null)
                .withDetail("replySubscription", replyEndpoint.subscription())
                .withDetail("pendingRequests", template.pendingRequestCount())
                .withDetail("reaperLastSweepAgeMs", reaperAgeMs)
                .withDetail("reaperHealthy", reaperHealthy)
                .build();
    }
}
