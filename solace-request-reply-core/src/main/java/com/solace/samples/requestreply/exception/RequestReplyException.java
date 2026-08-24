package com.solace.samples.requestreply.exception;

/** Base type for every failure raised by this library. */
public class RequestReplyException extends RuntimeException {
    public RequestReplyException(String message) { super(message); }
    public RequestReplyException(String message, Throwable cause) { super(message, cause); }
}
