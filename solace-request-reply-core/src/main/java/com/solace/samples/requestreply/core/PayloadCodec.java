package com.solace.samples.requestreply.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.solace.samples.requestreply.exception.RequestReplyException;

import java.nio.charset.StandardCharsets;

/**
 * Payload serialization. JSON by default, with byte arrays and Strings passed through
 * untouched so callers can layer any format they like on top.
 */
public class PayloadCodec {

    private final ObjectMapper mapper;

    public PayloadCodec(ObjectMapper mapper) { this.mapper = mapper; }

    public byte[] serialize(Object payload) {
        if (payload == null) { return new byte[0]; }
        if (payload instanceof byte[] b) { return b; }
        if (payload instanceof String s) { return s.getBytes(StandardCharsets.UTF_8); }
        try {
            return mapper.writeValueAsBytes(payload);
        } catch (Exception e) {
            throw new RequestReplyException("Could not serialize " + payload.getClass().getName(), e);
        }
    }

    @SuppressWarnings("unchecked")
    public <R> R deserialize(byte[] payload, Class<R> type) {
        if (type == byte[].class) { return (R) payload; }
        if (type == String.class) {
            return (R) (payload == null ? null : new String(payload, StandardCharsets.UTF_8));
        }
        if (payload == null || payload.length == 0) { return null; }
        try {
            return mapper.readValue(payload, type);
        } catch (Exception e) {
            throw new RequestReplyException("Could not deserialize a reply into "
                    + type.getName() + ": " + new String(payload, StandardCharsets.UTF_8), e);
        }
    }
}
