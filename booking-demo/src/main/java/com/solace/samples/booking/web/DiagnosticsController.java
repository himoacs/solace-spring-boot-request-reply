package com.solace.samples.booking.web;

import com.solace.samples.booking.replier.SeatInventoryService;
import com.solace.samples.requestreply.api.ReplyingSolaceTemplate;
import com.solace.samples.requestreply.config.SolaceRequestReplyProperties;
import com.solace.samples.requestreply.core.CorrelationStore;
import com.solace.samples.requestreply.endpoint.DmqProvisioner;
import com.solace.samples.requestreply.endpoint.ReplyEndpoint;
import com.solace.samples.requestreply.transport.SolaceSession;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Reports what was actually provisioned, rather than what was configured.
 *
 * <p>Worth its own endpoint because with {@code provision.mode=CREATE_IF_MISSING} "did my
 * configuration take effect?" is a real question, and because the reply endpoint's subscription
 * is the single most useful thing to see when replies stop arriving.
 */
@RestController
@RequestMapping("/api/diagnostics")
public class DiagnosticsController {

    private final SolaceSession session;
    /** Absent on a replier-only process, where reply.enabled=false removes both beans. */
    private final ObjectProvider<ReplyEndpoint> replyEndpointProvider;
    private final ObjectProvider<ReplyingSolaceTemplate> templateProvider;
    private final SolaceRequestReplyProperties props;
    private final CorrelationStore store;
    private final ObjectProvider<SeatInventoryService> inventory;
    private final DmqProvisioner dmq;

    public DiagnosticsController(SolaceSession session,
                                 ObjectProvider<ReplyEndpoint> replyEndpointProvider,
                                 ObjectProvider<ReplyingSolaceTemplate> templateProvider,
                                 SolaceRequestReplyProperties props, CorrelationStore store,
                                 ObjectProvider<SeatInventoryService> inventory,
                                 DmqProvisioner dmq) {
        this.session = session;
        this.replyEndpointProvider = replyEndpointProvider;
        this.templateProvider = templateProvider;
        this.props = props;
        this.store = store;
        this.inventory = inventory;
        this.dmq = dmq;
    }

    @GetMapping("/endpoints")
    public Map<String, Object> endpoints() {
        Map<String, Object> out = new LinkedHashMap<>();

        Map<String, Object> sess = new LinkedHashMap<>();
        sess.put("connected", session.isConnected());
        sess.put("lastEvent", session.lastEvent());
        sess.put("reconnects", session.reconnectCount());
        out.put("session", sess);

        ReplyEndpoint replyEndpoint = replyEndpointProvider.getIfAvailable();
        ReplyingSolaceTemplate template = templateProvider.getIfAvailable();
        Map<String, Object> reply = new LinkedHashMap<>();
        reply.put("enabled", props.getReply().isEnabled());
        if (replyEndpoint == null) {
            // Not a failure: a replier is never addressed on a reply queue, so it provisions none.
            reply.put("detail", "replier-only; no reply queue is provisioned");
        } else {
            reply.put("established", replyEndpoint.isEstablished());
            reply.put("queue", replyEndpoint.isEstablished() ? replyEndpoint.queue().getName() : null);
            reply.put("subscription", replyEndpoint.subscription());
            reply.put("replyToTemplate", template == null ? null : template.replyTopicPattern());
            reply.put("perRequestPlaceholders", props.getReply().getPerRequestPlaceholders());
        }
        out.put("replyEndpoint", reply);

        SolaceRequestReplyProperties.Replier r = props.getReplier();
        Map<String, Object> req = new LinkedHashMap<>();
        req.put("queue", r.getQueue());
        req.put("topics", r.getTopics());
        req.put("accessType", r.getAccessType());
        req.put("concurrency", r.getConcurrency());
        req.put("provisionMode", r.getProvision().getMode());
        req.put("maxRedelivery", r.getProvision().getMaxRedelivery());
        req.put("respectsTtl", r.getProvision().isRespectsTtl());
        out.put("requestQueue", req);

        Map<String, Object> flight = new LinkedHashMap<>();
        flight.put("pendingRequests", store.size());
        SeatInventoryService inv = inventory.getIfAvailable();
        if (inv != null) { flight.put("distinctReservations", inv.reservationCount()); }
        out.put("inFlight", flight);

        Map<String, Object> d = new LinkedHashMap<>();
        d.put("configuredEnabled", props.getDmq().isEnabled());
        // Separate from the flag because dead-lettering can be switched on and still be inert:
        // a DMQ that does not exist means the broker deletes instead of moving.
        d.put("established", dmq.isEstablished());
        d.put("queue", dmq.queueName());
        d.put("detail", dmq.detail());
        d.put("requestsEligible", props.getRequest().isDmqEligible());
        d.put("repliesEligible", props.getReplier().isDmqEligible());
        d.put("replyTtlMillis",
                props.getReplier().resolveReplyTtlMillis(props.getRequest().getTimeout()));
        out.put("dmq", d);

        return out;
    }

    /**
     * Whether this instance's reply path is up.
     *
     * <p>Worth its own endpoint because an instance that cannot receive replies still accepts
     * requests perfectly happily, and answers every one of them with a timeout.
     */
    @GetMapping("/reply-path")
    public Map<String, Object> replyPath() {
        ReplyEndpoint replyEndpoint = replyEndpointProvider.getIfAvailable();
        ReplyingSolaceTemplate template = templateProvider.getIfAvailable();
        Map<String, Object> out = new LinkedHashMap<>();
        if (replyEndpoint == null || template == null) {
            out.put("enabled", false);
            out.put("verdict", "replier-only; this process has no reply path and needs none");
            return out;
        }
        boolean ready = template.waitForReplyEndpoint(java.time.Duration.ofSeconds(2));
        out.put("established", replyEndpoint.isEstablished());
        out.put("ready", ready);
        out.put("queue", replyEndpoint.isEstablished() ? replyEndpoint.queue().getName() : null);
        out.put("subscription", replyEndpoint.subscription());
        out.put("verdict", ready ? "reply path is bound and subscribed"
                : "reply path is NOT ready — every request would time out");
        return out;
    }
}
