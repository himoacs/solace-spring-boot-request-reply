package com.solace.samples.requestreply.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.solace.samples.requestreply.api.ReplyingSolaceTemplate;
import com.solace.samples.requestreply.api.SolaceListenerErrorHandler;
import com.solace.samples.requestreply.core.CorrelationStore;
import com.solace.samples.requestreply.core.DefaultReplyingSolaceTemplate;
import com.solace.samples.requestreply.core.InMemoryCorrelationStore;
import com.solace.samples.requestreply.core.PayloadCodec;
import com.solace.samples.requestreply.endpoint.ReplyEndpoint;
import com.solace.samples.requestreply.endpoint.ReplyEndpointFactory;
import com.solace.samples.requestreply.endpoint.DmqProvisioner;
import com.solace.samples.requestreply.endpoint.RequestQueueProvisioner;
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
import org.springframework.beans.factory.BeanFactory;
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
 * Wires the library. Every bean is {@code @ConditionalOnMissingBean} so any piece can be replaced,
 * except {@link SolaceListenerAnnotationBeanPostProcessor}, which has to be registered as a static
 * bean before regular bean instantiation begins.
 *
 * <p>This is the only entry point. There is deliberately no {@code @EnableSolaceRequestReply}
 * counterpart to {@code @EnableKafka}: the post-processor it would import is useless without the
 * session, provisioner and publisher defined here, so importing it alone produced a half-built
 * context rather than an alternative to auto-configuration.
 */
@AutoConfiguration
@ConditionalOnClass(JCSMPSession.class)
@ConditionalOnProperty(prefix = "solace.request-reply", name = "enabled", havingValue = "true",
        matchIfMissing = true)
@EnableConfigurationProperties(SolaceRequestReplyProperties.class)
public class SolaceRequestReplyAutoConfiguration {

    private static final Logger log = LoggerFactory.getLogger(SolaceRequestReplyAutoConfiguration.class);

    private final SolaceRequestReplyProperties properties;

    public SolaceRequestReplyAutoConfiguration(SolaceRequestReplyProperties properties) {
        this.properties = properties;
    }

    /**
     * An absent bean is the wrong way to learn that a deliberate configuration choice took
     * effect, so the replier-only mode announces itself.
     */
    @jakarta.annotation.PostConstruct
    void announceReplyMode() {
        if (!properties.getReply().isEnabled()) {
            log.info("Reply endpoint disabled (solace.request-reply.reply.enabled=false): this "
                    + "process provisions no reply queue and cannot send requests. Replies to "
                    + "requests it handles still go to each request's own replyTo topic.");
        }
    }

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


    @Bean(destroyMethod = "close")
    @ConditionalOnMissingBean
    public PersistentPublisher persistentPublisher(SolaceSession session) {
        return new PersistentPublisher(session);
    }

    @Bean
    @ConditionalOnMissingBean
    public CorrelationStore correlationStore() {
        return new InMemoryCorrelationStore();
    }

    /**
     * The reply endpoint, and everything downstream of it, exist only for a process that sends
     * requests. A replier consumes the shared request queue and publishes to each request's
     * replyTo topic; nothing is ever addressed to a reply queue of its own.
     */
    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "solace.request-reply.reply", name = "enabled",
            havingValue = "true", matchIfMissing = true)
    public ReplyEndpoint replyEndpoint(SolaceSession session, SolaceRequestReplyProperties props) {
        return new ReplyEndpointFactory(session, props).create();
    }

    /**
     * Completion pool for reply futures, kept off the JCSMP dispatch thread so a slow
     * continuation registered by a caller cannot stall delivery of every other reply.
     */
    @Bean(name = "solaceCompletionExecutor", destroyMethod = "shutdown")
    @ConditionalOnMissingBean(name = "solaceCompletionExecutor")
    @ConditionalOnProperty(prefix = "solace.request-reply.reply", name = "enabled",
            havingValue = "true", matchIfMissing = true)
    public ExecutorService solaceCompletionExecutor() {
        return Executors.newFixedThreadPool(4, named("rr-complete-"));
    }

    @Bean(destroyMethod = "close")
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "solace.request-reply.reply", name = "enabled",
            havingValue = "true", matchIfMissing = true)
    public ReplyingSolaceTemplate replyingSolaceTemplate(
            SolaceSession session, ReplyEndpoint replyEndpoint, PersistentPublisher publisher,
            CorrelationStore store, SolaceRequestReplyProperties props, PayloadCodec codec,
            ExecutorService solaceCompletionExecutor) {
        DefaultReplyingSolaceTemplate template = new DefaultReplyingSolaceTemplate(
                session, replyEndpoint, publisher, store, props, codec,
                solaceCompletionExecutor);
        template.start();
        if (!template.waitForReplyEndpoint(props.getReply().getWaitForEndpoint())) {
            log.warn("Reply endpoint was not ready within {}; early requests may time out",
                    props.getReply().getWaitForEndpoint());
        }
        return template;
    }

    @Bean
    @ConditionalOnMissingBean(name = "solaceReplyPathHealthIndicator")
    @ConditionalOnClass(name = "org.springframework.boot.actuate.health.HealthIndicator")
    @ConditionalOnProperty(prefix = "solace.request-reply.reply", name = "enabled",
            havingValue = "true", matchIfMissing = true)
    public ReplyPathHealthIndicator solaceReplyPathHealthIndicator(
            SolaceSession session, ReplyEndpoint replyEndpoint, ReplyingSolaceTemplate template,
            SolaceRequestReplyProperties props) {
        return new ReplyPathHealthIndicator(session, replyEndpoint, template,
                props.getRequest().getReaperMaxStaleness().toMillis());
    }

    /**
     * Unconditional, unlike {@link #solaceReplyPathHealthIndicator}: a replier-only process has
     * no reply path to report on, and was otherwise left with no Solace health signal at all.
     */
    @Bean
    @ConditionalOnMissingBean(name = "solaceSessionHealthIndicator")
    @ConditionalOnClass(name = "org.springframework.boot.actuate.health.HealthIndicator")
    public SolaceSessionHealthIndicator solaceSessionHealthIndicator(
            SolaceSession session, SolaceListenerRegistrar registrar,
            SolaceListenerAnnotationBeanPostProcessor postProcessor) {
        return new SolaceSessionHealthIndicator(session, registrar, postProcessor);
    }


    /**
     * Provisioned eagerly at startup rather than lazily on first dead message, because by the
     * time a message needs the DMQ it is already too late — a missing DMQ means the broker has
     * deleted it.
     */
    @Bean
    @ConditionalOnMissingBean
    public DmqProvisioner dmqProvisioner(SolaceSession session, SolaceRequestReplyProperties props) {
        DmqProvisioner provisioner = new DmqProvisioner(session, props.getDmq());
        provisioner.ensure();
        return provisioner;
    }

    @Bean
    @ConditionalOnMissingBean
    public RequestQueueProvisioner requestQueueProvisioner(SolaceSession session,
                                                           SolaceRequestReplyProperties props) {
        return new RequestQueueProvisioner(session, props.getReplier());
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
    @ConditionalOnMissingBean
    public SolaceListenerRegistrar solaceListenerRegistrar(
            SolaceListenerAnnotationBeanPostProcessor postProcessor,
            SolaceSession session, RequestQueueProvisioner provisioner,
            PersistentPublisher publisher, PayloadCodec codec, HandlerMethodInvoker invoker,
            ObjectProvider<SolaceListenerErrorHandler> errorHandlers,
            BeanFactory beanFactory, SolaceRequestReplyProperties props) {
        return new SolaceListenerRegistrar(postProcessor, session, provisioner, publisher, codec,
                invoker, errorHandlers, beanFactory, props);
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
        private final ObjectProvider<SolaceListenerErrorHandler> errorHandlers;
        private final BeanFactory beanFactory;
        private final SolaceRequestReplyProperties props;
        private final List<SolaceMessageListenerContainer> containers = new ArrayList<>();

        SolaceListenerRegistrar(SolaceListenerAnnotationBeanPostProcessor postProcessor,
                                SolaceSession session, RequestQueueProvisioner provisioner,
                                PersistentPublisher publisher, PayloadCodec codec,
                                HandlerMethodInvoker invoker,
                                ObjectProvider<SolaceListenerErrorHandler> errorHandlers,
                                BeanFactory beanFactory, SolaceRequestReplyProperties props) {
            this.postProcessor = postProcessor;
            this.session = session;
            this.provisioner = provisioner;
            this.publisher = publisher;
            this.codec = codec;
            this.invoker = invoker;
            this.errorHandlers = errorHandlers;
            this.beanFactory = beanFactory;
            this.props = props;
        }

        @Override
        public void onApplicationEvent(ContextRefreshedEvent event) {
            if (!containers.isEmpty()) { return; }
            publisher.start();
            for (SolaceListenerEndpoint endpoint : postProcessor.endpoints()) {
                // Each container owns its own handler pool, sized from this endpoint's own
                // concurrency — see SolaceMessageListenerContainer's class javadoc for why one
                // process-wide pool was removed rather than kept alongside this.
                SolaceMessageListenerContainer container = new SolaceMessageListenerContainer(
                        endpoint, session, provisioner, publisher, codec, invoker,
                        resolveErrorHandler(endpoint),
                        props.getDmq().isEnabled() && props.getReplier().isDmqEligible(),
                        props.getReplier().resolveReplyTtlMillis(props.getRequest().getTimeout()),
                        props.getReplier().getBackpressure().resolve(endpoint.concurrency()));
                container.start();
                containers.add(container);
            }
            if (!containers.isEmpty()) {
                log.info("Started {} Solace listener container(s)", containers.size());
            }
        }

        /**
         * The handler named by {@code @SolaceListener(errorHandler = "...")}, or the single
         * {@link SolaceListenerErrorHandler} bean if the listener names none.
         *
         * <p>{@code getIfUnique} rather than {@code getIfAvailable}: with two handler beans and no
         * {@code @Primary}, the latter throws during context refresh naming neither the listener
         * nor the attribute. Ambiguity here should be a warning that says which listener to
         * annotate, not a startup failure with no address on it.
         */
        private SolaceListenerErrorHandler resolveErrorHandler(SolaceListenerEndpoint endpoint) {
            String name = endpoint.errorHandler();
            if (name != null && !name.isBlank()) {
                return beanFactory.getBean(name, SolaceListenerErrorHandler.class);
            }
            SolaceListenerErrorHandler unique = errorHandlers.getIfUnique();
            if (unique == null && errorHandlers.stream().findAny().isPresent()) {
                log.warn("Listener '{}' names no errorHandler and several "
                        + "SolaceListenerErrorHandler beans exist, so none is applied and handler "
                        + "exceptions become error replies. Name one with "
                        + "@SolaceListener(errorHandler = \"beanName\").", endpoint.id());
            }
            return unique;
        }

        public List<SolaceMessageListenerContainer> containers() { return List.copyOf(containers); }

        @Override
        public void close() {
            containers.forEach(SolaceMessageListenerContainer::close);
            containers.clear();
        }
    }
}
