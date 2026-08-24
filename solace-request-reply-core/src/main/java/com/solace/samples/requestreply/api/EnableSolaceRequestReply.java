package com.solace.samples.requestreply.api;

import com.solace.samples.requestreply.listener.SolaceListenerAnnotationBeanPostProcessor;
import org.springframework.context.annotation.Import;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Enables {@link SolaceListener} detection, after {@code @EnableKafka}.
 *
 * <p>Not required under Spring Boot, where auto-configuration registers the same
 * post-processor. Present for explicit configuration and for tests that bypass
 * auto-configuration.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Import(SolaceListenerAnnotationBeanPostProcessor.class)
public @interface EnableSolaceRequestReply {
}
