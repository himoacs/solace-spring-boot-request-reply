package com.solace.samples.requestreply.exception;

/** The replier handled the request and returned a failure rather than a result. */
public class RemoteErrorException extends RequestReplyException {

    private final String correlationId;
    private final String remoteMessage;

    public RemoteErrorException(String correlationId, String remoteMessage) {
        super("Remote error for correlationId=" + correlationId + ": " + remoteMessage);
        this.correlationId = correlationId;
        this.remoteMessage = remoteMessage;
    }

    public String getCorrelationId() { return correlationId; }
    public String getRemoteMessage() { return remoteMessage; }
}
