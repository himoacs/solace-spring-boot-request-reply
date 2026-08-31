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
import com.solacesystems.jcsmp.XMLMessage;
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
    private volatile boolean paused;

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
        paused = false;
        try {
            ConsumerFlowProperties fp = new ConsumerFlowProperties();
            fp.setEndpoint(queue);
            fp.setAckMode(clientAck
                    ? JCSMPProperties.SUPPORTED_MESSAGE_ACK_CLIENT
                    : JCSMPProperties.SUPPORTED_MESSAGE_ACK_AUTO);
            if (clientAck) {
                // Lets a handler settle a message FAILED to actively request redelivery, rather
                // than merely leaving it unacknowledged -- which only redelivers on disconnect,
                // not on demand. Only CLIENT-ack flows ever call settle() themselves.
                fp.addRequiredSettlementOutcomes(XMLMessage.Outcome.FAILED);
            }
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

    /**
     * Stops delivery on this flow without unbinding it — consumer-side backpressure, not
     * shutdown. The flow stays bound and any message already in flight to {@code handler} is
     * unaffected; the broker simply stops pushing more until {@link #resume()}.
     */
    public synchronized void pause() {
        FlowReceiver f = flow;
        if (f == null || paused) { return; }
        try {
            // Unlike start(), stop() declares no checked exception -- but the flow can still be
            // torn down concurrently by a reconnect, so guard against that the same way
            // acknowledge() does elsewhere in this library, rather than assuming it cannot happen.
            f.stop();
            paused = true;
        } catch (RuntimeException e) {
            log.warn("Could not pause flow '{}'; the broker will keep delivering at full window "
                    + "until the next attempt succeeds", name, e);
        }
    }

    /** Resumes delivery after {@link #pause()}. A no-op if the flow was never paused. */
    public synchronized void resume() {
        FlowReceiver f = flow;
        if (f == null || !paused) { return; }
        try {
            f.start();
            paused = false;
        } catch (JCSMPException e) {
            log.warn("Could not resume flow '{}'; it will stay paused until the next reconnect "
                    + "rebinds it, or the next resume attempt succeeds", name, e);
        }
    }

    public boolean isPaused() { return paused; }

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
