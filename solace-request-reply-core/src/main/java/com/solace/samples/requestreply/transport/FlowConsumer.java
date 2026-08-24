package com.solace.samples.requestreply.transport;

import com.solace.samples.requestreply.exception.TransportException;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.ConsumerFlowProperties;
import com.solacesystems.jcsmp.EndpointProperties;
import com.solacesystems.jcsmp.FlowEventArgs;
import com.solacesystems.jcsmp.FlowEventHandler;
import com.solacesystems.jcsmp.FlowReceiver;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.Queue;
import com.solacesystems.jcsmp.XMLMessageListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;

/**
 * One bound flow over a queue.
 *
 * <p>Deliberately hands the raw {@link BytesXMLMessage} to the handler rather than a converted
 * model, because with CLIENT acknowledgement the handler decides <em>when</em> to ack — and for
 * request/reply that has to be after the reply is safely published, not before. Acking first
 * risks a crash that loses the request while the work has already happened: a reserved seat and
 * a failure reported to the customer, with no recovery path.
 */
public class FlowConsumer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(FlowConsumer.class);

    private final SolaceSession session;
    private final Queue queue;
    private final Consumer<BytesXMLMessage> handler;
    private final boolean clientAck;
    private final String name;

    private volatile FlowReceiver flow;

    public FlowConsumer(SolaceSession session, Queue queue, String name,
                        boolean clientAck, Consumer<BytesXMLMessage> handler) {
        this.session = session;
        this.queue = queue;
        this.name = name;
        this.clientAck = clientAck;
        this.handler = handler;
    }

    public synchronized void start() {
        if (flow != null) { return; }
        try {
            ConsumerFlowProperties fp = new ConsumerFlowProperties();
            fp.setEndpoint(queue);
            fp.setAckMode(clientAck
                    ? JCSMPProperties.SUPPORTED_MESSAGE_ACK_CLIENT
                    : JCSMPProperties.SUPPORTED_MESSAGE_ACK_AUTO);
            // The queue already exists; do not let flow creation try to re-provision it.
            EndpointProperties ep = new EndpointProperties();
            flow = session.jcsmp().createFlow(new Listener(), fp, ep, new Events());
            flow.start();
            log.info("Flow '{}' bound to queue '{}' (ackMode={})",
                    name, queue.getName(), clientAck ? "CLIENT" : "AUTO");
        } catch (JCSMPException e) {
            throw new TransportException("Could not bind flow '" + name + "' to queue '"
                    + queue.getName() + "'", e);
        }
    }

    /** Rebinds after a reconnect; the queue may have been recreated underneath us. */
    public synchronized void rebind() {
        close();
        start();
    }

    public boolean isBound() { return flow != null; }

    @Override
    public synchronized void close() {
        FlowReceiver f = flow;
        flow = null;
        if (f != null) { f.close(); }
    }

    private final class Listener implements XMLMessageListener {
        @Override
        public void onReceive(BytesXMLMessage msg) {
            try {
                handler.accept(msg);
            } catch (RuntimeException ex) {
                log.error("Flow '{}' handler threw; message not acknowledged so it will be "
                        + "redelivered", name, ex);
            }
        }

        @Override
        public void onException(JCSMPException e) {
            log.error("Flow '{}' consumer exception", name, e);
        }
    }

    private final class Events implements FlowEventHandler {
        @Override
        public void handleEvent(Object source, FlowEventArgs event) {
            log.info("Flow '{}' event {}: {}", name, event.getEvent(), event.getInfo());
        }
    }
}
