package com.solace.samples.requestreply.config;

import com.solace.samples.requestreply.config.SolaceRequestReplyAutoConfiguration.SolaceListenerRegistrar;
import com.solace.samples.requestreply.listener.SolaceListenerAnnotationBeanPostProcessor;
import com.solace.samples.requestreply.listener.SolaceMessageListenerContainer;
import com.solace.samples.requestreply.transport.SolaceSession;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Reports the session and this process's {@code @SolaceListener} containers through Actuator.
 *
 * <p>Registered unconditionally, unlike {@link ReplyPathHealthIndicator}: that one is gated on
 * {@code reply.enabled}, so a replier-only process — the deployment mode the library steers you
 * toward for anything running under a Kubernetes Deployment — was left with no Solace health
 * signal at all. This covers it: whether the session is connected, and whether every discovered
 * listener actually has a running container, not merely a queue that was once provisioned.
 *
 * <h2>Starting is not the same as down</h2>
 * {@code SolaceListenerRegistrar} only populates its containers once the application context
 * finishes refreshing. Before that, "no containers" and "no listeners declared" look identical
 * unless something also counts what was <em>discovered</em>. This checks both, so a process with
 * listeners that have not started yet is reported DOWN — correct for a readiness probe — rather
 * than trivially UP because there was nothing yet to find not-running.
 *
 * <h2>Readiness, not liveness</h2>
 * This goes DOWN while the session is {@code RECONNECTING} — deliberately, so a readiness probe
 * takes the pod out of rotation rather than routing it work it cannot yet handle. Wiring it to a
 * <b>liveness</b> probe instead has a sharp edge: Kubernetes would kill the pod during the very
 * reconnect the library's retry budget ({@code java.reconnect-retries} x
 * {@code reconnect-retry-wait-in-millis}, 300s at the documented defaults) exists to survive,
 * turning a transient network blip into a restart storm. The same caution applies to
 * {@link ReplyPathHealthIndicator}, for the same reason.
 *
 * <h2>Backlog is informational, not a failure</h2>
 * Each running container's handler-queue depth and whether it currently has its flow(s) paused
 * for backpressure are reported as details, not folded into the up/down bit. A listener pausing
 * under load is deliberate, expected control flow — see {@code SolaceMessageListenerContainer}'s
 * Backpressure section — not a fault to take the pod out of rotation for; flipping readiness on
 * every pause would just add restart-storm-style flapping under the exact load spike this
 * mechanism exists to absorb gracefully.
 */
public class SolaceSessionHealthIndicator implements HealthIndicator {

    private final SolaceSession session;
    private final SolaceListenerRegistrar registrar;
    private final SolaceListenerAnnotationBeanPostProcessor postProcessor;

    public SolaceSessionHealthIndicator(SolaceSession session, SolaceListenerRegistrar registrar,
                                        SolaceListenerAnnotationBeanPostProcessor postProcessor) {
        this.session = session;
        this.registrar = registrar;
        this.postProcessor = postProcessor;
    }

    @Override
    public Health health() {
        boolean connected = session.isConnected();
        int declared = postProcessor.endpoints().size();
        List<SolaceMessageListenerContainer> containers = registrar.containers();
        boolean startingUp = declared > 0 && containers.isEmpty();

        List<String> notRunning = containers.stream()
                .filter(c -> !c.isRunning())
                .map(SolaceMessageListenerContainer::id)
                .collect(Collectors.toList());

        boolean up = connected && !startingUp && notRunning.isEmpty();

        Map<String, Object> backlogById = new LinkedHashMap<>();
        for (SolaceMessageListenerContainer c : containers) {
            backlogById.put(c.id(), Map.of(
                    "queueDepth", c.queueDepth(),
                    "backpressureEngaged", c.isBackpressureEngaged()));
        }

        Health.Builder builder = up ? Health.up() : Health.down();
        return builder
                .withDetail("sessionConnected", connected)
                .withDetail("lastSessionEvent", session.lastEvent())
                .withDetail("reconnects", session.reconnectCount())
                .withDetail("listenersDeclared", declared)
                .withDetail("listenersRunning", containers.size() - notRunning.size())
                .withDetail("listenersNotRunning", notRunning)
                .withDetail("startingUp", startingUp)
                .withDetail("listenerBacklog", backlogById)
                .build();
    }
}
