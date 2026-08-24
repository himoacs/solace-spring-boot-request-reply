package com.solace.samples.requestreply.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.solace.samples.requestreply.api.ReplyingSolaceTemplate;
import com.solace.samples.requestreply.api.SolaceListenerErrorHandler;
import com.solace.samples.requestreply.core.CorrelationStore;
import com.solace.samples.requestreply.core.DefaultReplyingSolaceTemplate;
import com.solace.samples.requestreply.core.InMemoryCorrelationStore;
import com.solace.samples.requestreply.core.PayloadCodec;
import com.solace.samples.requestreply.core.TracingContextBridge;
import com.solace.samples.requestreply.endpoint.ReplyEndpoint;
import com.solace.samples.requestreply.endpoint.ReplyEndpointFactory;
import com.solace.samples.requestreply.endpoint.RequestQueueProvisioner;
import com.solace.samples.requestreply.endpoint.SempClient;
import com.solace.samples.requestreply.latency.LatencyRecorder;
import com.solace.samples.requestreply.listener.HandlerMethodInvoker;
import com.solace.samples.requestreply.listener.SolaceListenerAnnotationBeanPostProcessor;
import com.solace.samples.requestreply.listener.SolaceListenerEndpoint;
import com.solace.samples.requestreply.listener.SolaceMessageListenerContainer;
import com.solace.samples.requestreply.transport.PersistentPublisher;
import com.solace.samples.requestreply.transport.SolaceSession;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.SpringJCSMPFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.messaging.converter.ByteArrayMessageConverter;
import org.springframework.messaging.converter.CompositeMessageConverter;
import org.springframework.messaging.converter.MappingJackson2MessageConverter;
import org.springframework.messaging.converter.StringMessageConverter;
import org.springframework.messaging.handler.annotation.support.DefaultMessageHandlerMethodFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Wires the library. Every bean is {@code @ConditionalOnMissingBean} so any piece can be replaced.
 */
@AutoConfiguration
@ConditionalOnClass(JCSMPSession.class)
@ConditionalOnProperty(prefix = "solace.request-reply", name = "enabled", havingValue = "true",
        matchIfMissing = true)
@EnableConfigurationProperties(SolaceRequestReplyProperties.class)
public class SolaceRequestReplyAutoConfiguration {

    private static final Logger log = LoggerFactory.getLogger(SolaceRequestReplyAutoConfiguration.class);

    @Bean(destroyMethod = "close")
    @ConditionalOnMissingBean
    public SolaceSession solaceSession(SpringJCSMPFactory factory) {
        SolaceSession session = new SolaceSession(factory);
        session.start();
        return session;
    }

    @Bean
    @ConditionalOnMissingBean
    public PayloadCodec payloadCodec(ObjectProvider<ObjectMapper> mapper) {
        return new PayloadCodec(mapper.getIfAvailable(ObjectMapper::new));
    }

    @Bean
    @ConditionalOnMissingBean
    public TracingContextBridge tracingContextBridge() {
        return TracingContextBridge.NOOP;
    }

    @Bean
    @ConditionalOnMissingBean
    public LatencyRecorder.Collecting latencyRecorder() {
        return new LatencyRecorder.Collecting();
    }

    @Bean(destroyMethod = "close")
    @ConditionalOnMissingBean
    public PersistentPublisher persistentPublisher(SolaceSession session,
                                                   SolaceRequestReplyProperties props) {
        return new PersistentPublisher(session, props.getRequest().getDeliveryMode());
    }

    @Bean
    @ConditionalOnMissingBean
    public CorrelationStore correlationStore() {
        return new InMemoryCorrelationStore();
    }

    @Bean
    @ConditionalOnMissingBean
    public ReplyEndpoint replyEndpoint(SolaceSession session, SolaceRequestReplyProperties props) {
        return new ReplyEndpointFactory(session, props).create();
    }

    /**
     * Completion pool for reply futures.
     *
     * <p>A plain JDK pool on purpose. The OpenTelemetry agent propagates context across
     * executors by an exact class-name allowlist that includes {@code ThreadPoolExecutor} but
     * not custom implementations, so the ordinary choice is also the one that keeps traces whole.
     */
    @Bean(name = "solaceCompletionExecutor", destroyMethod = "shutdown")
    @ConditionalOnMissingBean(name = "solaceCompletionExecutor")
    public ExecutorService solaceCompletionExecutor() {
        return Executors.newFixedThreadPool(4, named("rr-complete-"));
    }

    @Bean(name = "solaceHandlerExecutor", destroyMethod = "shutdown")
    @ConditionalOnMissingBean(name = "solaceHandlerExecutor")
    public ExecutorService solaceHandlerExecutor(SolaceRequestReplyProperties props) {
        int n = Math.max(1, props.getReplier().getConcurrency());
        return Executors.newFixedThreadPool(n, named("rr-handler-"));
    }

    @Bean(destroyMethod = "close")
    @ConditionalOnMissingBean
    public ReplyingSolaceTemplate replyingSolaceTemplate(
            SolaceSession session, ReplyEndpoint replyEndpoint, PersistentPublisher publisher,
            CorrelationStore store, SolaceRequestReplyProperties props, PayloadCodec codec,
            ExecutorService solaceCompletionExecutor, LatencyRecorder.Collecting latency,
            TracingContextBridge tracing) {
        DefaultReplyingSolaceTemplate template = new DefaultReplyingSolaceTemplate(
                session, replyEndpoint, publisher, store, props, codec,
                solaceCompletionExecutor, latency, tracing);
        template.start();
        if (!template.waitForReplyEndpoint(props.getReply().getWaitForEndpoint())) {
            log.warn("Reply endpoint was not ready within {}; early requests may time out",
                    props.getReply().getWaitForEndpoint());
        }
        return template;
    }

    @Bean
    @ConditionalOnMissingBean
    public SempClient sempClient(SolaceRequestReplyProperties props) {
        return new SempClient(props.getReplier().getPartitioning());
    }

    @Bean
    @ConditionalOnMissingBean
    public RequestQueueProvisioner requestQueueProvisioner(SolaceSession session,
                                                           SolaceRequestReplyProperties props,
                                                           SempClient semp) {
        return new RequestQueueProvisioner(session, props.getReplier(), semp);
    }

    @Bean
    @ConditionalOnMissingBean
    public DefaultMessageHandlerMethodFactory solaceHandlerMethodFactory(ObjectProvider<ObjectMapper> mapper) {
        DefaultMessageHandlerMethodFactory factory = new DefaultMessageHandlerMethodFactory();
        MappingJackson2MessageConverter json =
                new MappingJackson2MessageConverter(mapper.getIfAvailable(ObjectMapper::new));
        // Strict content-type matching would reject our payloads, which carry no MIME header.
        json.setStrictContentTypeMatch(false);
        factory.setMessageConverter(new CompositeMessageConverter(
                List.of(new ByteArrayMessageConverter(), new StringMessageConverter(), json)));
        factory.afterPropertiesSet();
        return factory;
    }

    @Bean
    @ConditionalOnMissingBean
    public HandlerMethodInvoker solaceHandlerMethodInvoker(DefaultMessageHandlerMethodFactory factory,
                                                           PayloadCodec codec) {
        return new HandlerMethodInvoker(factory, codec);
    }

    @Bean
    public static SolaceListenerAnnotationBeanPostProcessor solaceListenerAnnotationBeanPostProcessor() {
        return new SolaceListenerAnnotationBeanPostProcessor();
    }

    /**
     * Starts a container per discovered listener, once the context is up.
     *
     * <p>Deferred to {@code ContextRefreshedEvent} rather than bean creation so that binding a
     * flow — and therefore receiving the first request — cannot happen before the beans that
     * handler depends on exist.
     */
    @Bean
    public SolaceListenerRegistrar solaceListenerRegistrar(
            SolaceListenerAnnotationBeanPostProcessor postProcessor,
            SolaceSession session, RequestQueueProvisioner provisioner,
            PersistentPublisher publisher, PayloadCodec codec, HandlerMethodInvoker invoker,
            ExecutorService solaceHandlerExecutor, TracingContextBridge tracing,
            ObjectProvider<SolaceListenerErrorHandler> errorHandlers) {
        return new SolaceListenerRegistrar(postProcessor, session, provisioner, publisher, codec,
                invoker, solaceHandlerExecutor, tracing, errorHandlers);
    }

    private static ThreadFactory named(String prefix) {
        AtomicInteger n = new AtomicInteger();
        return r -> {
            Thread t = new Thread(r, prefix + n.incrementAndGet());
            t.setDaemon(true);
            return t;
        };
    }

    /** Starts and stops the listener containers with the application context. */
    public static class SolaceListenerRegistrar
            implements ApplicationListener<ContextRefreshedEvent>, AutoCloseable {

        private final SolaceListenerAnnotationBeanPostProcessor postProcessor;
        private final SolaceSession session;
        private final RequestQueueProvisioner provisioner;
        private final PersistentPublisher publisher;
        private final PayloadCodec codec;
        private final HandlerMethodInvoker invoker;
        private final ExecutorService handlerExecutor;
        private final TracingContextBridge tracing;
        private final ObjectProvider<SolaceListenerErrorHandler> errorHandlers;
        private final List<SolaceMessageListenerContainer> containers = new ArrayList<>();

        SolaceListenerRegistrar(SolaceListenerAnnotationBeanPostProcessor postProcessor,
                                SolaceSession session, RequestQueueProvisioner provisioner,
                                PersistentPublisher publisher, PayloadCodec codec,
                                HandlerMethodInvoker invoker, ExecutorService handlerExecutor,
                                TracingContextBridge tracing,
                                ObjectProvider<SolaceListenerErrorHandler> errorHandlers) {
            this.postProcessor = postProcessor;
            this.session = session;
            this.provisioner = provisioner;
            this.publisher = publisher;
            this.codec = codec;
            this.invoker = invoker;
            this.handlerExecutor = handlerExecutor;
            this.tracing = tracing;
            this.errorHandlers = errorHandlers;
        }

        @Override
        public void onApplicationEvent(ContextRefreshedEvent event) {
            if (!containers.isEmpty()) { return; }
            publisher.start();
            for (SolaceListenerEndpoint endpoint : postProcessor.endpoints()) {
                SolaceMessageListenerContainer container = new SolaceMessageListenerContainer(
                        endpoint, session, provisioner, publisher, codec, invoker,
                        errorHandlers.getIfAvailable(), handlerExecutor, tracing);
                container.start();
                containers.add(container);
            }
            if (!containers.isEmpty()) {
                log.info("Started {} Solace listener container(s)", containers.size());
            }
        }

        public List<SolaceMessageListenerContainer> containers() { return List.copyOf(containers); }

        @Override
        public void close() {
            containers.forEach(SolaceMessageListenerContainer::close);
            containers.clear();
        }
    }
}
