package com.solace.samples.requestreply.exception;

/** A publish, connect or subscribe operation failed at the transport level. */
public class TransportException extends RequestReplyException {
    public TransportException(String message) { super(message); }
    public TransportException(String message, Throwable cause) { super(message, cause); }
}
