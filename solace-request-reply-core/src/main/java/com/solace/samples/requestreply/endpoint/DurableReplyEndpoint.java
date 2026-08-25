package com.solace.samples.requestreply.endpoint;

import com.solace.samples.requestreply.exception.EndpointProvisioningException;
import com.solace.samples.requestreply.transport.SolaceSession;
import com.solacesystems.jcsmp.EndpointProperties;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.Queue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A durable, exclusive queue holding this instance's replies.
 *
 * <p>Exclusive because replies are addressed rather than shared: exactly one consumer, this
 * instance. That makes the queue name load-bearing — two processes resolving the same
 * {@code instanceId} bind the same queue, the second as a standby that receives nothing, and
 * every one of its requests times out. {@code ReplyEndpointFactory} logs the resolved id for
 * exactly that reason.
 *
 * <p>Durable also means the queue outlives the process. An instance whose identity changes
 * between runs leaves the old queue behind, still accumulating replies nobody will read, so
 * {@code instanceId} should be stable for a given instance — a pod name, not a random value.
 */
public class DurableReplyEndpoint implements ReplyEndpoint {

    private static final Logger log = LoggerFactory.getLogger(DurableReplyEndpoint.class);

    private final SolaceSession session;
    private final ReplyTopicPattern topicPattern;
    private final String name;
    private final int quotaMb;
    private final AtomicBoolean established = new AtomicBoolean(false);
    private volatile Queue queue;

    DurableReplyEndpoint(SolaceSession session, ReplyTopicPattern topicPattern,
                         String name, int quotaMb) {
        this.session = session;
        this.topicPattern = topicPattern;
        this.name = name;
        this.quotaMb = quotaMb;
    }

    @Override
    public void establish() {
        JCSMPSession jcsmp = session.jcsmp();
        String sub = topicPattern.subscription();
        try {
            Queue q = JCSMPFactory.onlyInstance().createQueue(name);
            EndpointProperties props = new EndpointProperties();
            props.setPermission(EndpointProperties.PERMISSION_CONSUME);
            props.setAccessType(EndpointProperties.ACCESSTYPE_EXCLUSIVE);
            props.setQuota(quotaMb);
            props.setRespectsMsgTTL(Boolean.TRUE);
            // Idempotent, but FLAG_IGNORE_ALREADY_EXISTS suppresses only "already exists": a
            // queue whose properties differ still raises PropertyMismatchException, which is
            // what makes configuration drift loud rather than silent.
            jcsmp.provision(q, props,
                    JCSMPSession.FLAG_IGNORE_ALREADY_EXISTS | JCSMPSession.WAIT_FOR_CONFIRM);

            // WAIT_FOR_CONFIRM here, unlike on direct subscriptions where omitting it is what
            // lets REAPPLY_SUBSCRIPTIONS work. Reapply never covers endpoints, so there is no
            // upside to omitting it and every reason to know the subscribe actually landed.
            jcsmp.addSubscription(q, JCSMPFactory.onlyInstance().createTopic(sub),
                    JCSMPSession.WAIT_FOR_CONFIRM);

            queue = q;
            established.set(true);
            log.info("Reply endpoint ready: queue={} subscription={}", name, sub);

        } catch (JCSMPException e) {
            established.set(false);
            throw new EndpointProvisioningException(name,
                    "could not establish the durable reply endpoint with subscription '" + sub + "'", e);
        }
    }

    @Override
    public Queue queue() {
        Queue q = queue;
        if (q == null) {
            throw new EndpointProvisioningException(name, "endpoint not established yet");
        }
        return q;
    }

    @Override
    public String replyTopic(Map<String, String> perRequestValues) {
        return topicPattern.resolve(perRequestValues);
    }

    @Override
    public String subscription() { return topicPattern.subscription(); }

    @Override
    public boolean isEstablished() { return established.get(); }

    @Override
    public void close() {
        established.set(false);
        queue = null;
    }
}
