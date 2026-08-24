package com.solace.samples.requestreply.transport;

import com.solace.samples.requestreply.api.PublishResult;

import java.util.concurrent.CompletableFuture;

/**
 * Per-publish state, attached to the message as its <b>correlation key</b>.
 *
 * <p>Two correlation concepts are in play and conflating them is a classic bug:
 * <ul>
 *   <li>{@code correlationKey} — this object. Local only, never serialized, never on the wire.
 *       It exists so the asynchronous broker acknowledgement can be matched back to the
 *       {@code send()} that caused it.</li>
 *   <li>{@code correlationId} — a String on the wire. It exists so a reply can be matched to
 *       its request.</li>
 * </ul>
 *
 * <p>{@code setCorrelationKey} takes an arbitrary Object, which is what lets the send future
 * ride along on the message itself rather than living in a side map.
 */
public final class PublishTicket {

    private final String correlationId;
    private final String topic;
    private final long startNanos;
    private final CompletableFuture<PublishResult> sendFuture = new CompletableFuture<>();

    public PublishTicket(String correlationId, String topic, long startNanos) {
        this.correlationId = correlationId;
        this.topic = topic;
        this.startNanos = startNanos;
    }

    public String correlationId() { return correlationId; }
    public String topic() { return topic; }
    public CompletableFuture<PublishResult> sendFuture() { return sendFuture; }

    void completeSpooled() {
        sendFuture.complete(new PublishResult(correlationId, topic, System.nanoTime() - startNanos));
    }

    void completeFailed(Throwable cause) { sendFuture.completeExceptionally(cause); }
}
