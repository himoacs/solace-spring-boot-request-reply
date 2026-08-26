package com.solace.samples.requestreply.api;

/**
 * Converts a handler exception into a reply, after {@code KafkaListenerErrorHandler}.
 *
 * <p>Returning a value publishes it as the reply. Rethrowing forwards the failure to the
 * requestor as a {@code RemoteErrorException}, which is the default when no handler is set —
 * the requestor fails fast instead of waiting out its timeout for a reply that will never come.
 *
 * <p>Rethrowing {@link RetryableHandlerException} gets no special treatment here — it is
 * converted to a {@code RemoteErrorException} exactly like any other rethrown exception, not
 * turned into a redelivery. That type only triggers a redelivery when the original handler
 * method throws it directly, before this handler is ever consulted; see its javadoc for why
 * that decision deliberately does not extend to here.
 */
public interface SolaceListenerErrorHandler {

    Object handleError(RequestReplyMessage request, Exception exception) throws Exception;
}
