package com.solace.samples.requestreply.listener;

import com.solace.samples.requestreply.api.SolaceListener;

import java.lang.reflect.Method;
import java.util.List;

/**
 * A discovered {@link SolaceListener} method and everything needed to bind it.
 *
 * @param id           container id, for logging and management
 * @param bean         the bean owning the handler
 * @param method       the handler method
 * @param queue        queue to bind — the consumer group
 * @param topics       subscriptions to map onto the queue
 * @param concurrency  flows to bind, and the size of this listener's own handler pool
 * @param clientAck    CLIENT acknowledgement, required for reply-then-ack ordering
 * @param replyTo      explicit @SendTo destination, or null to use the request's reply-to
 * @param sendReply    whether the return value should be published at all
 * @param errorHandler bean name of a SolaceListenerErrorHandler, or empty
 */
public record SolaceListenerEndpoint(
        String id,
        Object bean,
        Method method,
        String queue,
        List<String> topics,
        int concurrency,
        boolean clientAck,
        String replyTo,
        boolean sendReply,
        String errorHandler) {
}
