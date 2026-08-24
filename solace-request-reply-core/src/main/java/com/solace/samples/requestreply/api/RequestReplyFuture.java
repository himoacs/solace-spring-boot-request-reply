package com.solace.samples.requestreply.api;

import java.util.concurrent.CompletableFuture;

/**
 * The reply future, carrying a nested future for the publish itself.
 *
 * <p>Shaped after Spring Kafka's {@code RequestReplyFuture}, and for the same reason: a
 * request/reply call has <b>two</b> independent failure points, and one future cannot
 * express both.
 *
 * <ul>
 *   <li>{@link #getSendFuture()} completes when the broker has spooled the request, or
 *       fails if it never got there — a full spool, a missing permission, a rejected publish.</li>
 *   <li>This future completes with the correlated reply, or fails with
 *       {@code RequestTimeoutException} or {@code RemoteErrorException}.</li>
 * </ul>
 *
 * <p>Collapsing the two makes "the broker rejected my publish" indistinguishable from
 * "nobody answered" — both surface as the same timeout, pointing at the wrong half of the
 * system. Ignore {@code getSendFuture()} and you get exactly that single-outcome behaviour;
 * it is there when the distinction matters.
 *
 * <p>A publish failure also fails this future, since waiting for a reply to a request that
 * never landed is pointless.
 */
public class RequestReplyFuture<R> extends CompletableFuture<R> {

    private final CompletableFuture<PublishResult> sendFuture;

    public RequestReplyFuture(CompletableFuture<PublishResult> sendFuture) {
        this.sendFuture = sendFuture;
    }

    /** Completes when the broker acknowledges the request as spooled. */
    public CompletableFuture<PublishResult> getSendFuture() { return sendFuture; }
}
