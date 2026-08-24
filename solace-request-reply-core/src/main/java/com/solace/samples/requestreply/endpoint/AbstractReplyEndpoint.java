package com.solace.samples.requestreply.endpoint;

import com.solace.samples.requestreply.exception.EndpointProvisioningException;
import com.solace.samples.requestreply.transport.SolaceSession;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.Queue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/** Shared establish / re-establish flow; subclasses differ only in how the queue is created. */
abstract class AbstractReplyEndpoint implements ReplyEndpoint {

    private static final Logger log = LoggerFactory.getLogger(AbstractReplyEndpoint.class);

    final SolaceSession session;
    final ReplyTopicPattern topicPattern;
    private final AtomicBoolean established = new AtomicBoolean(false);
    volatile Queue queue;

    AbstractReplyEndpoint(SolaceSession session, ReplyTopicPattern topicPattern) {
        this.session = session;
        this.topicPattern = topicPattern;
    }

    /** Creates the queue, however this endpoint kind does that. */
    protected abstract Queue createQueue(JCSMPSession jcsmp) throws JCSMPException;

    protected abstract String kind();

    /**
     * Creates the queue. The topic subscription is <em>not</em> applied here: it is passed to
     * the flow and applied as part of the bind.
     *
     * <p>That ordering is forced by temporary queues, which do not exist on the broker until a
     * flow binds to them — subscribing first fails with "Unknown Queue", subcode 20. Doing both
     * in the bind also removes any window in which the endpoint exists but matches nothing.
     */
    @Override
    public void establish() {
        JCSMPSession jcsmp = session.jcsmp();
        try {
            queue = createQueue(jcsmp);
            established.set(true);
            log.info("Reply endpoint prepared: kind={} queue={} subscription={} (applied at bind)",
                    kind(), queue.getName(), topicPattern.subscription());
        } catch (JCSMPException e) {
            established.set(false);
            throw new EndpointProvisioningException(
                    queue != null ? queue.getName() : "(reply)",
                    "could not create the " + kind() + " reply endpoint", e);
        }
    }

    @Override
    public void applySubscription() {
        String sub = topicPattern.subscription();
        try {
            // WAIT_FOR_CONFIRM here, unlike on direct subscriptions where omitting it is what lets
            // REAPPLY_SUBSCRIPTIONS work. Reapply never covers endpoints, so there is no upside to
            // omitting it and every reason to know the subscribe actually landed.
            session.jcsmp().addSubscription(queue(),
                    JCSMPFactory.onlyInstance().createTopic(sub),
                    JCSMPSession.WAIT_FOR_CONFIRM);
            log.info("Reply subscription applied: queue={} topic={}", queue().getName(), sub);
        } catch (JCSMPException e) {
            established.set(false);
            throw new EndpointProvisioningException(queue().getName(),
                    "could not map reply subscription '" + sub + "'", e);
        }
    }

    @Override
    public void reestablish() {
        established.set(false);
        log.info("Re-establishing {} reply endpoint after reconnect", kind());
        establish();
    }

    @Override
    public Queue queue() {
        Queue q = queue;
        if (q == null) {
            throw new EndpointProvisioningException("(reply)", "endpoint not established yet");
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
