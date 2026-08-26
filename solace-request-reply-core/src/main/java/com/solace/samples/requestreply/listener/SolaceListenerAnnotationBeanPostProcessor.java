package com.solace.samples.requestreply.listener;

import com.solace.samples.requestreply.api.SolaceListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.core.MethodIntrospector;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.core.env.Environment;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.util.StringUtils;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Finds {@link SolaceListener} methods and registers them, after Spring Kafka's equivalent
 * post-processor.
 *
 * <p>Reads Spring's own {@link SendTo} rather than defining a parallel annotation — the same
 * choice Spring Kafka and Spring AMQP make. {@code @SendTo} with no value means "reply to the
 * request's reply-to destination", which is both Spring Kafka's default and the only sensible
 * one here, since the requestor's address is dynamic.
 */
public class SolaceListenerAnnotationBeanPostProcessor implements BeanPostProcessor, BeanFactoryAware {

    private static final Logger log = LoggerFactory.getLogger(SolaceListenerAnnotationBeanPostProcessor.class);

    private final List<SolaceListenerEndpoint> endpoints = new ArrayList<>();
    private final AtomicInteger counter = new AtomicInteger();
    private BeanFactory beanFactory;
    private Environment environment;

    @Override
    public void setBeanFactory(BeanFactory beanFactory) {
        this.beanFactory = beanFactory;
        if (beanFactory instanceof org.springframework.beans.factory.ListableBeanFactory lbf) {
            try {
                this.environment = lbf.getBean(Environment.class);
            } catch (RuntimeException ignored) {
                // No Environment: placeholder resolution falls back to literals.
            }
        }
    }

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) {
        Class<?> target = org.springframework.aop.support.AopUtils.getTargetClass(bean);
        Map<Method, SolaceListener> found = MethodIntrospector.selectMethods(target,
                (MethodIntrospector.MetadataLookup<SolaceListener>) method ->
                        AnnotatedElementUtils.findMergedAnnotation(method, SolaceListener.class));
        found.forEach((method, annotation) -> endpoints.add(build(bean, method, annotation)));
        return bean;
    }

    private SolaceListenerEndpoint build(Object bean, Method method, SolaceListener ann) {
        SendTo sendTo = AnnotatedElementUtils.findMergedAnnotation(method, SendTo.class);
        boolean sendReply = sendTo != null && method.getReturnType() != void.class;
        String replyTo = null;
        if (sendTo != null && sendTo.value().length > 0 && StringUtils.hasText(sendTo.value()[0])) {
            replyTo = resolve(sendTo.value()[0]);
        }

        String id = StringUtils.hasText(ann.id())
                ? resolve(ann.id())
                : method.getDeclaringClass().getSimpleName() + "#" + method.getName()
                        + "-" + counter.incrementAndGet();

        int concurrency = 1;
        String rawConcurrency = resolve(ann.concurrency());
        if (StringUtils.hasText(rawConcurrency)) {
            try {
                concurrency = Math.max(1, Integer.parseInt(rawConcurrency.trim()));
            } catch (NumberFormatException e) {
                log.warn("Listener '{}': concurrency '{}' is not a number; using 1", id, rawConcurrency);
            }
        }

        List<String> topics = Arrays.stream(ann.topics()).map(this::resolve).filter(StringUtils::hasText).toList();

        return new SolaceListenerEndpoint(id, bean, method,
                resolve(ann.queue()), topics, concurrency,
                !"AUTO".equalsIgnoreCase(resolve(ann.ackMode())),
                replyTo, sendReply, resolve(ann.errorHandler()));
    }

    /**
     * Resolves a {@code ${...}} property placeholder. Deliberately not SpEL: Spring Kafka's
     * {@code @SendTo} also accepts {@code #{...}} (evaluated against the bean factory) and
     * {@code !{...}} (evaluated against the inbound record) — the latter exists there because a
     * Kafka record has no built-in reply-to, so SpEL is how you compute one. This library
     * already carries a dynamic reply-to on every request, so an unqualified {@code @SendTo}
     * gets that for free without needing SpEL at all; the only case SpEL would otherwise serve, a
     * fixed override destination, is already a {@code ${...}} placeholder away. Rather than
     * silently treating {@code #{...}}/{@code !{...}} as a literal string and misrouting every
     * reply to it, fail fast here so the mistake surfaces at startup, not as a request that times
     * out with no clue why.
     */
    private String resolve(String value) {
        if (value == null || value.isBlank()) { return value; }
        if (value.contains("#{") || value.contains("!{")) {
            throw new IllegalStateException("'" + value + "' looks like a SpEL expression, "
                    + "which this library does not support here (only '${...}' property "
                    + "placeholders are resolved). @SendTo with no value already routes to the "
                    + "request's own dynamic reply-to; for a fixed destination, use a literal or "
                    + "a '${...}' placeholder instead.");
        }
        return environment == null ? value : environment.resolvePlaceholders(value);
    }

    /** Endpoints discovered during context refresh. */
    public List<SolaceListenerEndpoint> endpoints() { return List.copyOf(endpoints); }
}
