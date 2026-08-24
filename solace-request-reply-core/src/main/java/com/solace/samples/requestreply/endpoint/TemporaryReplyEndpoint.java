package com.solace.samples.requestreply.endpoint;

import com.solace.samples.requestreply.transport.SolaceSession;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.Queue;

/**
 * Non-durable reply endpoint. The default, because a first run should need no provisioning
 * and leave nothing behind to clean up.
 *
 * <p>Named rather than anonymous: {@code createTemporaryQueue(String)} lets the queue carry
 * the instance id, so it is identifiable in the broker's queue list during a support call
 * instead of being an opaque generated string.
 *
 * <p>The hazard this kind carries: a temporary endpoint survives a disconnect for only 60
 * seconds, or 180 across an HA failover, and is then destroyed. On reconnect the broker
 * recreates it but does <em>not</em> restore its topic subscription, so replies land nowhere
 * while everything reports healthy. {@code reestablish()} plus the canary is what makes that
 * state detectable rather than silent.
 */
class TemporaryReplyEndpoint extends AbstractReplyEndpoint {

    private final String name;

    TemporaryReplyEndpoint(SolaceSession session, ReplyTopicPattern pattern, String name) {
        super(session, pattern);
        this.name = name;
    }

    @Override
    protected Queue createQueue(JCSMPSession jcsmp) throws JCSMPException {
        return jcsmp.createTemporaryQueue(name);
    }

    @Override
    protected String kind() { return "TEMPORARY"; }
}
