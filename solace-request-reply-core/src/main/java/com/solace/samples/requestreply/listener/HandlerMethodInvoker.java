package com.solace.samples.requestreply.listener;

import com.solace.samples.requestreply.api.RequestReplyMessage;
import com.solace.samples.requestreply.api.SolaceHeaders;
import com.solace.samples.requestreply.core.PayloadCodec;
import org.springframework.messaging.Message;
import org.springframework.messaging.handler.annotation.support.DefaultMessageHandlerMethodFactory;
import org.springframework.messaging.handler.invocation.InvocableHandlerMethod;
import org.springframework.messaging.support.MessageBuilder;

import java.util.HashMap;
import java.util.Map;

/**
 * Invokes a handler method with Spring's own argument resolution.
 *
 * <p>Deliberately built on {@link DefaultMessageHandlerMethodFactory} rather than hand-rolled.
 * That is the same machinery Spring Kafka and Spring AMQP use, so {@code @Payload},
 * {@code @Header}, {@code @Headers} and {@code Message<?>} parameters behave exactly as a
 * Spring developer expects — rather than merely resembling it, which is the failure mode of a
 * bespoke resolver that handles the common cases and surprises people on the rest.
 */
public class HandlerMethodInvoker {

    private final DefaultMessageHandlerMethodFactory factory;
    private final PayloadCodec codec;

    public HandlerMethodInvoker(DefaultMessageHandlerMethodFactory factory, PayloadCodec codec) {
        this.factory = factory;
        this.codec = codec;
    }

    public Object invoke(SolaceListenerEndpoint endpoint, RequestReplyMessage request) throws Exception {
        InvocableHandlerMethod handler = factory.createInvocableHandlerMethod(
                endpoint.bean(), endpoint.method());
        return handler.invoke(toSpringMessage(request));
    }

    /**
     * Maps the transport model onto a Spring message.
     *
     * <p>The payload stays raw bytes so Spring's converter performs the conversion to the
     * handler's declared parameter type, which is what makes {@code @Payload BookingRequest}
     * work without the library knowing about the domain.
     */
    private Message<?> toSpringMessage(RequestReplyMessage request) {
        Map<String, Object> headers = new HashMap<>(request.getHeaders());
        headers.put(SolaceHeaders.CORRELATION_ID, request.getCorrelationId());
        if (request.getReplyTo() != null) { headers.put(SolaceHeaders.REPLY_TO, request.getReplyTo()); }
        if (request.getSequence() != null) { headers.put(SolaceHeaders.SEQUENCE, request.getSequence()); }
        headers.put("solace_rawMessage", request);
        byte[] payload = request.getPayload() == null ? new byte[0] : request.getPayload();
        return MessageBuilder.withPayload(payload).copyHeaders(headers).build();
    }

    PayloadCodec codec() { return codec; }
}
