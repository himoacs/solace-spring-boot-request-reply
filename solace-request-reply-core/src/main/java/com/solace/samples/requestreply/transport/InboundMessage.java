package com.solace.samples.requestreply.transport;

import com.solace.samples.requestreply.api.RequestReplyMessage;
import com.solace.samples.requestreply.api.SolaceHeaders;
import com.solacesystems.jcsmp.BytesMessage;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.SDTException;
import com.solacesystems.jcsmp.SDTMap;
import com.solacesystems.jcsmp.TextMessage;

import java.nio.charset.StandardCharsets;

/** Converts an inbound JCSMP message into the transport-neutral model. */
public final class InboundMessage {

    private InboundMessage() { }

    public static RequestReplyMessage toModel(BytesXMLMessage msg) {
        RequestReplyMessage model = new RequestReplyMessage(payloadOf(msg));
        model.setCorrelationId(msg.getCorrelationId());
        if (msg.getReplyTo() != null) {
            model.setReplyTo(msg.getReplyTo().getName());
        }
        SDTMap sdt = msg.getProperties();
        if (sdt == null) { return model; }
        for (String key : sdt.keySet()) {
            try {
                switch (key) {
                    case SolaceHeaders.CONTENT_TYPE -> model.setContentType(sdt.getString(key));
                    case SolaceHeaders.PARTITION_KEY -> model.setPartitionKey(sdt.getString(key));
                    case SolaceHeaders.SEQUENCE -> model.setSequence(sdt.getLong(key));
                    case SolaceHeaders.ERROR -> {
                        if (Boolean.TRUE.equals(sdt.getBoolean(key))) { model.asError("remote error"); }
                    }
                    case SolaceHeaders.ERROR_MESSAGE -> {
                        String m = sdt.getString(key);
                        if (m != null) { model.asError(m); }
                    }
                    default -> {
                        Object v = sdt.get(key);
                        if (v != null) { model.addHeader(key, String.valueOf(v)); }
                    }
                }
            } catch (SDTException ignored) {
                // A malformed property must not cost us the payload. Best practice is explicit
                // about this: handle unexpected formats rather than letting them become poison.
            }
        }
        return model;
    }

    private static byte[] payloadOf(BytesXMLMessage msg) {
        if (msg instanceof BytesMessage b) {
            byte[] d = b.getData();
            return d == null ? new byte[0] : d;
        }
        if (msg instanceof TextMessage t) {
            String s = t.getText();
            return s == null ? new byte[0] : s.getBytes(StandardCharsets.UTF_8);
        }
        byte[] attachment = new byte[msg.getAttachmentContentLength()];
        msg.readAttachmentBytes(attachment);
        return attachment;
    }
}
