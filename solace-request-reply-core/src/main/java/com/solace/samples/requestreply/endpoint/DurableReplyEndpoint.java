package com.solace.samples.requestreply.endpoint;

import com.solace.samples.requestreply.transport.SolaceSession;
import com.solacesystems.jcsmp.EndpointProperties;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.Queue;

/**
 * Durable, exclusive reply endpoint, provisioned by the client.
 *
 * <p>Recommended for production: the topic subscription is a broker-side object, so it
 * survives reconnects outright and the temporary-queue silent-death mode cannot occur. In
 * exchange it needs a stable instance identity — a StatefulSet ordinal rather than a random
 * pod suffix — or every rescheduled pod leaves an orphan queue accumulating replies.
 *
 * <p>Provisioning is idempotent through {@code FLAG_IGNORE_ALREADY_EXISTS}, but that flag
 * suppresses only "already exists"; a queue whose properties differ still raises
 * {@code PropertyMismatchException}, which is what makes drift loud rather than silent.
 */
class DurableReplyEndpoint extends AbstractReplyEndpoint {

    private final String name;
    private final int quotaMb;

    DurableReplyEndpoint(SolaceSession session, ReplyTopicPattern pattern, String name, int quotaMb) {
        super(session, pattern);
        this.name = name;
        this.quotaMb = quotaMb;
    }

    @Override
    protected Queue createQueue(JCSMPSession jcsmp) throws JCSMPException {
        Queue q = JCSMPFactory.onlyInstance().createQueue(name);
        EndpointProperties props = new EndpointProperties();
        props.setPermission(EndpointProperties.PERMISSION_CONSUME);
        // Exclusive: exactly one consumer, this instance. Replies are addressed, not shared.
        props.setAccessType(EndpointProperties.ACCESSTYPE_EXCLUSIVE);
        props.setQuota(quotaMb);
        props.setRespectsMsgTTL(Boolean.TRUE);
        jcsmp.provision(q, props, JCSMPSession.FLAG_IGNORE_ALREADY_EXISTS | JCSMPSession.WAIT_FOR_CONFIRM);
        return q;
    }

    @Override
    protected String kind() { return "DURABLE"; }
}
