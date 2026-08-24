package com.solace.samples.booking.web;

import com.solace.samples.booking.replier.SeatInventoryService;
import com.solace.samples.requestreply.api.ReplyingSolaceTemplate;
import com.solace.samples.requestreply.config.SolaceRequestReplyProperties;
import com.solace.samples.requestreply.core.CorrelationStore;
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
    private final ReplyEndpoint replyEndpoint;
    private final ReplyingSolaceTemplate template;
    private final SolaceRequestReplyProperties props;
    private final CorrelationStore store;
    private final ObjectProvider<SeatInventoryService> inventory;

    public DiagnosticsController(SolaceSession session, ReplyEndpoint replyEndpoint,
                                 ReplyingSolaceTemplate template,
                                 SolaceRequestReplyProperties props, CorrelationStore store,
                                 ObjectProvider<SeatInventoryService> inventory) {
        this.session = session;
        this.replyEndpoint = replyEndpoint;
        this.template = template;
        this.props = props;
        this.store = store;
        this.inventory = inventory;
    }

    @GetMapping("/endpoints")
    public Map<String, Object> endpoints() {
        Map<String, Object> out = new LinkedHashMap<>();

        Map<String, Object> sess = new LinkedHashMap<>();
        sess.put("connected", session.isConnected());
        sess.put("lastEvent", session.lastEvent());
        sess.put("reconnects", session.reconnectCount());
        out.put("session", sess);

        Map<String, Object> reply = new LinkedHashMap<>();
        reply.put("type", props.getReply().getEndpointType());
        reply.put("established", replyEndpoint.isEstablished());
        reply.put("queue", replyEndpoint.isEstablished() ? replyEndpoint.queue().getName() : null);
        reply.put("subscription", replyEndpoint.subscription());
        reply.put("replyToTemplate", template.replyTopic());
        reply.put("perRequestPlaceholders", props.getReply().getPerRequestPlaceholders());
        reply.put("recreateOnReconnect", props.getReply().isRecreateOnReconnect());
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
        req.put("partitionCount", r.getPartitioning().getPartitionCount());
        req.put("partitioned", r.getPartitioning().getPartitionCount() > 0);
        out.put("requestQueue", req);

        Map<String, Object> flight = new LinkedHashMap<>();
        flight.put("pendingRequests", store.size());
        SeatInventoryService inv = inventory.getIfAvailable();
        if (inv != null) { flight.put("distinctReservations", inv.reservationCount()); }
        out.put("inFlight", flight);

        return out;
    }

    /**
     * Round-trips a probe through this instance's own reply path.
     *
     * <p>Exists because of one specific failure: a temporary reply queue destroyed past its
     * linger window is recreated by the broker <em>without</em> its subscription. The session is
     * up, the flow is bound, nothing logs an error, and every request times out for ever. This
     * turns that state into an answer.
     */
    @GetMapping("/reply-path")
    public Map<String, Object> replyPath() {
        boolean ready = template.waitForReplyEndpoint(java.time.Duration.ofSeconds(2));
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("established", replyEndpoint.isEstablished());
        out.put("ready", ready);
        out.put("queue", replyEndpoint.isEstablished() ? replyEndpoint.queue().getName() : null);
        out.put("subscription", replyEndpoint.subscription());
        out.put("verdict", ready ? "reply path is bound and subscribed"
                : "reply path is NOT ready — replies would be lost");
        return out;
    }
}
