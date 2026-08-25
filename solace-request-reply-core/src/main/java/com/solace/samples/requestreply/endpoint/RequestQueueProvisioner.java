package com.solace.samples.requestreply.endpoint;

import com.solace.samples.requestreply.config.SolaceRequestReplyProperties;
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
 * <h2>Why the modes collapse to one call</h2>
 * A spike against a live broker settled how {@code provision} behaves (see {@code spike/}):
 * {@code FLAG_IGNORE_ALREADY_EXISTS} suppresses only "already exists", subcode 33. A queue
 * that exists with <em>different</em> properties still raises
 * {@link PropertyMismatchException}, which carries the offending property name and the value
 * the broker actually holds.
 *
 * <p>So drift is loud rather than silent, and {@code CREATE_IF_MISSING} is safe to ship as the
 * default. It also means {@code CREATE_IF_MISSING} and {@code VALIDATE} issue the *same* call
 * and differ only in whether a missing queue is an error — two modes, one code path, and no
 * SEMP required to detect drift.
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
        ProvisionMode mode = cfg.getProvision().getMode();

        if (mode != ProvisionMode.OFF) {
            provision(queue, queueName, mode);
        } else {
            log.info("Request queue '{}': provision mode OFF, assuming it exists", queueName);
        }

        subscribe(queue, topics);
        return queue;
    }

    private void provision(Queue queue, String queueName, ProvisionMode mode) {
        EndpointProperties props = new EndpointProperties();
        props.setPermission(EndpointProperties.PERMISSION_CONSUME);
        props.setAccessType("EXCLUSIVE".equalsIgnoreCase(cfg.getAccessType())
                ? EndpointProperties.ACCESSTYPE_EXCLUSIVE
                : EndpointProperties.ACCESSTYPE_NONEXCLUSIVE);
        props.setQuota(cfg.getProvision().getQuotaMb());
        props.setMaxMsgRedelivery(cfg.getProvision().getMaxRedelivery());
        props.setRespectsMsgTTL(cfg.getProvision().isRespectsTtl());
        props.setDiscardBehavior(cfg.getProvision().isDiscardNotifySender()
                ? EndpointProperties.DISCARD_NOTIFY_SENDER_ON
                : EndpointProperties.DISCARD_NOTIFY_SENDER_OFF);

        long flags = JCSMPSession.WAIT_FOR_CONFIRM
                | (mode == ProvisionMode.CREATE_IF_MISSING ? JCSMPSession.FLAG_IGNORE_ALREADY_EXISTS : 0);

        try {
            session.jcsmp().provision(queue, props, flags);
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
            if (mode == ProvisionMode.VALIDATE
                    && e.getSubcodeEx() == com.solacesystems.jcsmp.JCSMPErrorResponseSubcodeEx.UNKNOWN_QUEUE_NAME) {
                throw new EndpointProvisioningException(queueName,
                        "does not exist and provision.mode=VALIDATE will not create it. "
                                + "Create it, or use CREATE_IF_MISSING.", e);
            }
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
