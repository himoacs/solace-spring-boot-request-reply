package com.solace.samples.requestreply.transport;

import com.solace.samples.requestreply.exception.TransportException;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.SessionEventArgs;
import com.solacesystems.jcsmp.SessionEventHandler;
import com.solacesystems.jcsmp.SpringJCSMPFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Owns the single {@link JCSMPSession} and broadcasts its lifecycle.
 *
 * <p>One session for the whole application, connected at startup and held for its lifetime,
 * which is what the JCSMP best-practice guidance calls for: session establishment costs
 * orders of magnitude more than publishing a message, and churn produces connection storms
 * during faults.
 *
 * <p>Reconnect listeners exist because the interesting recovery work is not the session —
 * JCSMP re-establishes that itself — but the <em>endpoints</em>. Subscriptions on queues are
 * never reapplied by {@code REAPPLY_SUBSCRIPTIONS}, which covers direct topic subscriptions
 * only, so something has to notice and redo them.
 */
public class SolaceSession implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(SolaceSession.class);

    private final SpringJCSMPFactory factory;
    private final List<Runnable> reconnectListeners = new CopyOnWriteArrayList<>();
    private final AtomicBoolean connected = new AtomicBoolean(false);
    private final AtomicLong reconnectCount = new AtomicLong();

    private volatile JCSMPSession session;
    private volatile String lastEvent = "NONE";

    public SolaceSession(SpringJCSMPFactory factory) {
        this.factory = factory;
    }

    public synchronized void start() {
        if (session != null) { return; }
        try {
            session = factory.createSession(null, new Events());
            session.connect();
            connected.set(true);
            lastEvent = "CONNECTED";
            log.info("Solace session connected: host={} vpn={}",
                    session.getProperty(com.solacesystems.jcsmp.JCSMPProperties.HOST),
                    session.getProperty(com.solacesystems.jcsmp.JCSMPProperties.VPN_NAME));
        } catch (JCSMPException e) {
            throw new TransportException("Failed to connect the Solace session", e);
        }
    }

    public JCSMPSession jcsmp() {
        JCSMPSession s = session;
        if (s == null) {
            throw new TransportException("Solace session not started");
        }
        return s;
    }

    public boolean isConnected() {
        JCSMPSession s = session;
        return connected.get() && s != null && !s.isClosed();
    }

    public String lastEvent() { return lastEvent; }

    public long reconnectCount() { return reconnectCount.get(); }

    /** Invoked after every successful reconnect, on the session event thread. */
    public void onReconnect(Runnable listener) { reconnectListeners.add(listener); }

    @Override
    public synchronized void close() {
        connected.set(false);
        JCSMPSession s = session;
        session = null;
        if (s != null && !s.isClosed()) {
            s.closeSession();
            log.info("Solace session closed");
        }
    }

    private final class Events implements SessionEventHandler {
        @Override
        public void handleEvent(SessionEventArgs event) {
            String info = event.getInfo();
            lastEvent = String.valueOf(event.getEvent());
            switch (event.getEvent()) {
                case RECONNECTING -> {
                    connected.set(false);
                    log.warn("Solace session RECONNECTING: {}", info);
                }
                case RECONNECTED -> {
                    connected.set(true);
                    reconnectCount.incrementAndGet();
                    log.info("Solace session RECONNECTED ({}): {}", reconnectCount.get(), info);
                    for (Runnable r : reconnectListeners) {
                        try {
                            r.run();
                        } catch (RuntimeException ex) {
                            // A failing listener must not stop the others: each one is a
                            // separate endpoint, and losing all of them because the first
                            // threw is how the prototype's recovery silently did nothing.
                            log.error("Reconnect listener failed", ex);
                        }
                    }
                }
                case DOWN_ERROR -> {
                    connected.set(false);
                    log.error("Solace session DOWN: {}", info);
                }
                default -> log.info("Solace session event {}: {}", event.getEvent(), info);
            }
        }
    }
}
