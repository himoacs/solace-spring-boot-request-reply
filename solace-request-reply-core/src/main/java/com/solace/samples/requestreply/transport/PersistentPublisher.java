package com.solace.samples.requestreply.transport;

import com.solace.samples.requestreply.api.RequestReplyMessage;
import com.solace.samples.requestreply.api.SolaceHeaders;
import com.solace.samples.requestreply.core.TracingContextBridge;
import com.solace.samples.requestreply.exception.TransportException;
import com.solacesystems.jcsmp.BytesMessage;
import com.solacesystems.jcsmp.DeliveryMode;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPStreamingPublishCorrelatingEventHandler;
import com.solacesystems.jcsmp.SDTMap;
import com.solacesystems.jcsmp.Topic;
import com.solacesystems.jcsmp.XMLMessageProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.function.Consumer;

/**
 * Publishes with PERSISTENT delivery and surfaces the broker's acknowledgement.
 *
 * <p>{@code producer.send()} returns as soon as the message reaches the transport, <em>before</em>
 * the broker has spooled it. Confirmation arrives later on
 * {@link JCSMPStreamingPublishCorrelatingEventHandler}. Wiring that up is what makes a rejected
 * publish — a full spool, a missing permission — report itself immediately instead of
 * masquerading as a reply timeout seconds later.
 */
public class PersistentPublisher implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(PersistentPublisher.class);

    private final SolaceSession session;
    private final DeliveryMode deliveryMode;
    private final TracingContextBridge tracing;
    private volatile XMLMessageProducer producer;

    public PersistentPublisher(SolaceSession session, String deliveryMode) {
        this(session, deliveryMode, TracingContextBridge.NOOP);
    }

    public PersistentPublisher(SolaceSession session, String deliveryMode, TracingContextBridge tracing) {
        this.session = session;
        this.deliveryMode = "DIRECT".equalsIgnoreCase(deliveryMode)
                ? DeliveryMode.DIRECT : DeliveryMode.PERSISTENT;
        this.tracing = tracing == null ? TracingContextBridge.NOOP : tracing;
    }

    public synchronized void start() {
        if (producer != null) { return; }
        try {
            producer = session.jcsmp().getMessageProducer(new Acks());
            log.info("Publisher started, deliveryMode={}", deliveryMode);
        } catch (JCSMPException e) {
            throw new TransportException("Could not create the message producer", e);
        }
    }

    /**
     * Publishes {@code message} to {@code topic}.
     *
     * @param ticket carries the send future; completed when the broker acknowledges
     * @param ttlMillis message TTL, or 0 for none. Setting it equal to the request timeout stops
     *                  a replier acting on a request the requestor has already given up on.
     */
    public void publish(String topic, RequestReplyMessage message, PublishTicket ticket, long ttlMillis) {
        XMLMessageProducer p = producer;
        if (p == null) { throw new TransportException("Publisher not started"); }
        try {
            BytesMessage msg = JCSMPFactory.onlyInstance().createMessage(BytesMessage.class);
            byte[] payload = message.getPayload();
            msg.setData(payload == null ? new byte[0] : payload);
            msg.setDeliveryMode(deliveryMode);
            if (ttlMillis > 0) { msg.setTimeToLive(ttlMillis); }

            if (message.getCorrelationId() != null) { msg.setCorrelationId(message.getCorrelationId()); }
            if (message.getReplyTo() != null) {
                msg.setReplyTo(JCSMPFactory.onlyInstance().createTopic(message.getReplyTo()));
            }
            if (ticket != null) { msg.setCorrelationKey(ticket); }

            SDTMap sdt = JCSMPFactory.onlyInstance().createMap();
            if (message.getContentType() != null) {
                sdt.putString(SolaceHeaders.CONTENT_TYPE, message.getContentType());
            }
            // Always stamped. A flat queue ignores it, so enabling partitioning later is a
            // queue-side change with no application redeploy.
            if (message.getPartitionKey() != null) {
                sdt.putString(SolaceHeaders.PARTITION_KEY, message.getPartitionKey());
            }
            if (message.getSequence() != null) {
                sdt.putLong(SolaceHeaders.SEQUENCE, message.getSequence());
            }
            if (message.isError()) {
                sdt.putBoolean(SolaceHeaders.ERROR, Boolean.TRUE);
                sdt.putString(SolaceHeaders.ERROR_MESSAGE,
                        message.getErrorMessage() == null ? "remote error" : message.getErrorMessage());
            }
            for (Map.Entry<String, String> h : message.getHeaders().entrySet()) {
                // rr_rt_* are reply-topic placeholder values, consumed while building the
                // reply-to. They are internal bookkeeping and have no business on the wire.
                if (h.getKey().startsWith("rr_rt_")) { continue; }
                sdt.putString(h.getKey(), h.getValue());
            }
            msg.setProperties(sdt);

            // Injected after the properties are set and immediately before the send, so the
            // span that is current at publish time is the one carried on the wire.
            tracing.inject(msg);

            Topic dest = JCSMPFactory.onlyInstance().createTopic(topic);
            p.send(msg, dest);

        } catch (JCSMPException e) {
            TransportException wrapped = new TransportException("Publish to '" + topic + "' failed", e);
            if (ticket != null) { ticket.completeFailed(wrapped); }
            throw wrapped;
        }
    }

    @Override
    public synchronized void close() {
        XMLMessageProducer p = producer;
        producer = null;
        if (p != null) { p.close(); }
    }

    /** Completes or fails the ticket that rode along on the message. */
    private final class Acks implements JCSMPStreamingPublishCorrelatingEventHandler {
        @Override
        public void responseReceivedEx(Object key) {
            if (key instanceof PublishTicket t) { t.completeSpooled(); }
        }

        @Override
        public void handleErrorEx(Object key, JCSMPException cause, long timestamp) {
            if (key instanceof PublishTicket t) {
                t.completeFailed(new TransportException(
                        "Broker rejected the publish to '" + t.topic() + "'", cause));
            } else {
                log.error("Publish failed with no ticket attached", cause);
            }
        }
    }
}
