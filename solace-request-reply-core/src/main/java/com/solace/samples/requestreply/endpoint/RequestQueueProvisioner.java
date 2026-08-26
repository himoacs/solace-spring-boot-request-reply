package com.solace.samples.requestreply.endpoint;

import com.solace.samples.requestreply.config.SolaceRequestReplyProperties;
import com.solace.samples.requestreply.config.SolaceRequestReplyProperties.AccessType;
import com.solace.samples.requestreply.config.SolaceRequestReplyProperties.ProvisionMode;
import com.solace.samples.requestreply.exception.EndpointProvisioningException;
import com.solace.samples.requestreply.transport.SolaceSession;
import com.solacesystems.jcsmp.EndpointProperties;
import com.solacesystems.jcsmp.JCSMPErrorResponseException;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.PropertyMismatchException;
import com.solacesystems.jcsmp.Queue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Creates or verifies the shared request queue and maps its topic subscriptions.
 *
 * <h2>Why there is only one provisioning call</h2>
 * A spike against a live broker settled how {@code provision} behaves (see {@code spike/}):
 * {@code FLAG_IGNORE_ALREADY_EXISTS} suppresses only "already exists", subcode 33 — it has no
 * bearing on whether a <em>missing</em> queue gets created. {@code provision()} creates a
 * missing endpoint unconditionally, flag or no flag; the flag only decides whether an
 * <em>existing, matching</em> queue is tolerated or rejected. A queue that exists with
 * <em>different</em> properties still raises {@link PropertyMismatchException} regardless,
 * carrying the offending property name and the value the broker actually holds.
 *
 * <p>That ruled out ever offering a "validate without creating" mode honestly — there is no
 * flag for it, and probing existence some other way first is machinery this library does not
 * carry for a mode whose entire appeal was supposed to be doing less, not more. So drift
 * detection is a property of {@code CREATE_IF_MISSING} alone, and it is loud rather than silent,
 * which is what makes {@code CREATE_IF_MISSING} safe to ship as the default.
 */
public class RequestQueueProvisioner {

    private static final Logger log = LoggerFactory.getLogger(RequestQueueProvisioner.class);

    private final SolaceSession session;
    private final SolaceRequestReplyProperties.Replier cfg;

    public RequestQueueProvisioner(SolaceSession session,
                                   SolaceRequestReplyProperties.Replier cfg) {
        this.session = session;
        this.cfg = cfg;
    }

    /** @return the queue, ready to bind flows against. */
    public Queue ensure(String queueName, List<String> topics) {
        Queue queue = JCSMPFactory.onlyInstance().createQueue(queueName);

        if (cfg.getProvision().getMode() != ProvisionMode.OFF) {
            provision(queue, queueName);
        } else {
            log.info("Request queue '{}': provision mode OFF, assuming it exists", queueName);
        }

        subscribe(queue, topics);
        return queue;
    }

    private void provision(Queue queue, String queueName) {
        EndpointProperties props = new EndpointProperties();
        props.setPermission(EndpointProperties.PERMISSION_CONSUME);
        props.setAccessType(cfg.getAccessType() == AccessType.EXCLUSIVE
                ? EndpointProperties.ACCESSTYPE_EXCLUSIVE
                : EndpointProperties.ACCESSTYPE_NONEXCLUSIVE);
        props.setQuota(cfg.getProvision().getQuotaMb());
        props.setMaxMsgRedelivery(cfg.getProvision().getMaxRedelivery());
        props.setRespectsMsgTTL(cfg.getProvision().isRespectsTtl());
        props.setDiscardBehavior(cfg.getProvision().isDiscardNotifySender()
                ? EndpointProperties.DISCARD_NOTIFY_SENDER_ON
                : EndpointProperties.DISCARD_NOTIFY_SENDER_OFF);

        try {
            session.jcsmp().provision(queue, props,
                    JCSMPSession.WAIT_FOR_CONFIRM | JCSMPSession.FLAG_IGNORE_ALREADY_EXISTS);
            log.info("Request queue '{}' provisioned/verified: accessType={} quota={}MB "
                            + "maxRedelivery={} respectsTtl={} notifySenderOnDiscard={}",
                    queueName, cfg.getAccessType(), cfg.getProvision().getQuotaMb(),
                    cfg.getProvision().getMaxRedelivery(), cfg.getProvision().isRespectsTtl(),
                    cfg.getProvision().isDiscardNotifySender());

        } catch (PropertyMismatchException e) {
            // The whole reason CREATE_IF_MISSING is safe: configuration and reality having
            // diverged is a startup failure that names the property, not a silent no-op.
            throw new EndpointProvisioningException(queueName,
                    "exists but its configuration has drifted — property '" + e.getProperty()
                            + "' on the broker is '" + e.getPropertyValue() + "', which does not match "
                            + "solace.request-reply.replier.provision. Reconcile the queue, or set "
                            + "provision.mode=OFF to accept the broker's settings.", e);

        } catch (JCSMPErrorResponseException e) {
            throw new EndpointProvisioningException(queueName,
                    "provisioning failed (subcode=" + e.getSubcodeEx() + ", '" + e.getResponsePhrase() + "')", e);

        } catch (JCSMPException e) {
            throw new EndpointProvisioningException(queueName, "provisioning failed", e);
        }
    }

    private void subscribe(Queue queue, List<String> topics) {
        if (topics == null || topics.isEmpty()) {
            log.warn("Request queue '{}' has no topic subscriptions configured; it will receive "
                    + "nothing unless something else maps topics onto it", queue.getName());
            return;
        }
        for (String topic : topics) {
            try {
                session.jcsmp().addSubscription(queue,
                        JCSMPFactory.onlyInstance().createTopic(topic),
                        JCSMPSession.WAIT_FOR_CONFIRM);
                log.info("Mapped topic '{}' onto queue '{}'", topic, queue.getName());
            } catch (JCSMPErrorResponseException e) {
                if (e.getSubcodeEx() == com.solacesystems.jcsmp.JCSMPErrorResponseSubcodeEx.SUBSCRIPTION_ALREADY_PRESENT) {
                    log.debug("Subscription '{}' already present on '{}'", topic, queue.getName());
                    continue;
                }
                throw new EndpointProvisioningException(queue.getName(),
                        "could not map topic '" + topic + "'", e);
            } catch (JCSMPException e) {
                throw new EndpointProvisioningException(queue.getName(),
                        "could not map topic '" + topic + "'", e);
            }
        }
    }
}
