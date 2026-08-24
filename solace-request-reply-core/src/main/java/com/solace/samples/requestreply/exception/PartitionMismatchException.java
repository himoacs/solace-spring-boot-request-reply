package com.solace.samples.requestreply.exception;

/**
 * The queue exists but its partition count does not match configuration.
 *
 * <p>Deliberately fatal rather than self-correcting: changing partition count on a live
 * queue requires draining it first, and decreasing it <em>deletes</em> the messages held
 * in removed partitions. An application must not do that to itself on a redeploy.
 */
public class PartitionMismatchException extends RequestReplyException {

    public PartitionMismatchException(String queue, int configured, int actual) {
        super("Queue '" + queue + "' has partitionCount=" + actual + " but configuration expects "
                + configured + ". Changing it requires draining the queue first, and decreasing it "
                + "deletes messages in the removed partitions, so this is not applied automatically. "
                + "Fix the queue with SEMP, or set solace.request-reply.replier.partitioning"
                + ".allow-partition-resize=true to accept that risk.");
    }
}
