package com.solace.samples.requestreply.endpoint;

import com.solace.samples.requestreply.config.SolaceRequestReplyProperties;
import com.solace.samples.requestreply.transport.SolaceSession;
import com.solacesystems.jcsmp.EndpointProperties;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.Queue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Ensures the dead message queue exists.
 *
 * <p>Without it the whole feature is inert: a broker that has nowhere to put a dead message
 * deletes it, whether or not the publisher marked it eligible. Provisioning is therefore part
 * of enabling dead-lettering, not a separate operational step.
 *
 * <p>Plain JCSMP, no SEMP. {@code #DEAD_MSG_QUEUE} is an ordinary durable queue despite the
 * reserved-looking name, and JCSMP provisions it like any other — verified against a live
 * broker. That is what keeps this working for temporary reply endpoints too: every queue points
 * at the Message VPN's default DMQ already, so nothing has to set {@code deadMsgQueue} per
 * endpoint, which is the one part SEMP would have been needed for.
 *
 * <h2>Why failure here is not fatal</h2>
 * Dead-lettering is on by default, so a broker that refuses this — a restricted client profile,
 * a DMQ owned by another team, a name the deployment does not permit — must not take the
 * application down with it. The consequence of failing is the behaviour that existed before
 * this feature: dead messages are discarded. That is worth one clear warning, not a crash.
 */
public class DmqProvisioner {

    private static final Logger log = LoggerFactory.getLogger(DmqProvisioner.class);

    private final SolaceSession session;
    private final SolaceRequestReplyProperties.Dmq cfg;

    private volatile boolean established;
    private volatile String detail = "not attempted";

    public DmqProvisioner(SolaceSession session, SolaceRequestReplyProperties.Dmq cfg) {
        this.session = session;
        this.cfg = cfg;
    }

    /** True when the DMQ is known to exist, so dead-lettering will actually happen. */
    public boolean isEstablished() { return established; }

    /** Human-readable outcome, surfaced through diagnostics and health. */
    public String detail() { return detail; }

    public String queueName() { return cfg.getName(); }

    public void ensure() {
        if (!cfg.isEnabled()) {
            detail = "disabled; dead messages are discarded";
            log.debug("DMQ support disabled");
            return;
        }
        if (!cfg.isProvision()) {
            // Assume it was created out of band. Nothing is verified here, because a client
            // without management access cannot tell "missing" from "not visible to me".
            established = true;
            detail = "provisioning skipped; assuming '" + cfg.getName() + "' exists";
            log.info("DMQ provisioning skipped by configuration; assuming '{}' exists", cfg.getName());
            return;
        }
        try {
            Queue q = JCSMPFactory.onlyInstance().createQueue(cfg.getName());
            EndpointProperties props = new EndpointProperties();
            props.setPermission(EndpointProperties.PERMISSION_CONSUME);
            // Non-exclusive so several operators can browse it at once during an incident.
            props.setAccessType(EndpointProperties.ACCESSTYPE_NONEXCLUSIVE);
            props.setQuota(cfg.getQuotaMb());
            // Emphatically NOT respectsMsgTTL: these messages are here because they expired.
            // Honouring their TTL again would expire them straight back out of the DMQ, which
            // is the one place they are supposed to survive.
            props.setRespectsMsgTTL(Boolean.FALSE);

            session.jcsmp().provision(q, props,
                    JCSMPSession.FLAG_IGNORE_ALREADY_EXISTS | JCSMPSession.WAIT_FOR_CONFIRM);

            established = true;
            detail = "provisioned/verified";
            log.info("Dead message queue '{}' provisioned/verified: quota={}MB respectsTtl=false",
                    cfg.getName(), cfg.getQuotaMb());

        } catch (Exception e) {
            established = false;
            detail = "unavailable: " + rootMessage(e);
            log.warn("Could not provision the dead message queue '{}': {}. Expired and "
                            + "redelivery-exhausted messages will be DISCARDED rather than kept. "
                            + "Create the queue out of band and set "
                            + "solace.request-reply.dmq.provision=false, or set dmq.enabled=false "
                            + "to stop trying.",
                    cfg.getName(), rootMessage(e));
        }
    }

    private static String rootMessage(Throwable t) {
        Throwable c = t;
        while (c.getCause() != null) { c = c.getCause(); }
        return c.getMessage() == null ? c.getClass().getSimpleName() : c.getMessage();
    }
}
