package com.solace.samples.requestreply.exception;

/**
 * An endpoint could not be created, validated, subscribed or bound.
 *
 * <p>Carries the endpoint name so startup failures name the queue that is wrong rather
 * than making the operator correlate a stack trace against configuration.
 */
public class EndpointProvisioningException extends RequestReplyException {

    private final String endpointName;

    public EndpointProvisioningException(String endpointName, String message) {
        super("Endpoint '" + endpointName + "': " + message);
        this.endpointName = endpointName;
    }

    public EndpointProvisioningException(String endpointName, String message, Throwable cause) {
        super("Endpoint '" + endpointName + "': " + message, cause);
        this.endpointName = endpointName;
    }

    public String getEndpointName() { return endpointName; }
}
