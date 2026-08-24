package com.solace.samples.requestreply.api;

/**
 * Converts a handler exception into a reply, after {@code KafkaListenerErrorHandler}.
 *
 * <p>Returning a value publishes it as the reply. Rethrowing forwards the failure to the
 * requestor as a {@code RemoteErrorException}, which is the default when no handler is set —
 * the requestor fails fast instead of waiting out its timeout for a reply that will never come.
 */
public interface SolaceListenerErrorHandler {

    Object handleError(RequestReplyMessage request, Exception exception) throws Exception;
}
