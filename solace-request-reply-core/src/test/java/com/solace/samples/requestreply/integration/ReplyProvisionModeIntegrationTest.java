package com.solace.samples.requestreply.integration;

import com.solace.samples.requestreply.api.ReplyingSolaceTemplate;
import com.solace.samples.requestreply.config.SolaceRequestReplyAutoConfiguration;
import com.solace.samples.requestreply.support.SolaceTestBroker;
import com.solacesystems.jcsmp.EndpointProperties;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.Queue;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@code reply.provision-mode}, mirroring {@code replier.provision.mode}.
 *
 * <p>Before this, the reply endpoint always provisioned — there was no way to tell it "this
 * queue is provisioned for you, do not try." That left the requestor with no equivalent of
 * {@code replier.provision.mode: OFF}, on a message VPN whose client profile forbids creating
 * endpoints the replier could already adopt cleanly.
 */
class ReplyProvisionModeIntegrationTest {

    private static final String INSTANCE = "provision-mode-test";
    private static final String QUEUE = "q.test.provisionmode.reply." + INSTANCE;

    private static JCSMPSession rawSession;

    @BeforeAll
    static void connect() throws Exception {
        JCSMPProperties p = new JCSMPProperties();
        p.setProperty(JCSMPProperties.HOST, SolaceTestBroker.smfHost());
        p.setProperty(JCSMPProperties.VPN_NAME, SolaceTestBroker.vpn());
        p.setProperty(JCSMPProperties.USERNAME, SolaceTestBroker.username());
        p.setProperty(JCSMPProperties.PASSWORD, SolaceTestBroker.password());
        rawSession = JCSMPFactory.onlyInstance().createSession(p);
        rawSession.connect();
    }

    @AfterAll
    static void disconnect() {
        if (rawSession != null) { rawSession.closeSession(); }
    }

    private ApplicationContextRunner runner() {
        return new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(
                        com.solace.spring.boot.autoconfigure.SolaceJavaAutoConfiguration.class,
                        SolaceRequestReplyAutoConfiguration.class))
                .withPropertyValues(
                        "solace.java.host=" + SolaceTestBroker.smfHost(),
                        "solace.java.msg-vpn=" + SolaceTestBroker.vpn(),
                        "solace.java.client-username=" + SolaceTestBroker.username(),
                        "solace.java.client-password=" + SolaceTestBroker.password(),
                        "solace.request-reply.request.timeout=5s",
                        "solace.request-reply.reply.instance-id=" + INSTANCE,
                        "solace.request-reply.reply.queue-name-pattern=" + QUEUE,
                        "solace.request-reply.replier.queue=q.test.provisionmode.requests",
                        "solace.request-reply.replier.topics=test/provisionmode/v1/>");
    }

    @Test
    void offAdoptsAPreProvisionedQueueWithoutTryingToReconcileIt() throws Exception {
        // A quota the Spring context is never told about. If OFF attempted to provision at all
        // -- even with the ignore-already-exists flag -- a differing quota would still raise
        // PropertyMismatchException, exactly as ProvisionDriftIntegrationTest demonstrates. OFF
        // succeeding here is only possible because it skips provisioning entirely.
        provisionRaw(QUEUE, 55);
        try {
            runner().withPropertyValues(
                            "solace.request-reply.reply.provision-mode=OFF",
                            "solace.request-reply.reply.quota-mb=999")
                    .run(context -> {
                        assertThat(context).hasNotFailed();
                        ReplyingSolaceTemplate template = context.getBean(ReplyingSolaceTemplate.class);
                        assertThat(template.waitForReplyEndpoint(Duration.ofSeconds(15)))
                                .as("OFF must adopt the existing queue, quota mismatch and all")
                                .isTrue();
                    });
        } finally {
            deprovisionRaw(QUEUE);
        }
    }

    @Test
    void createIfMissingFailsClearlyOnDriftRatherThanSilentlyAcceptingIt() throws Exception {
        // A quota the Spring context is told something different about. CREATE_IF_MISSING must
        // still call provision() even though the queue already exists, which is exactly what
        // makes drift detection work regardless of whether creation was needed.
        provisionRaw(QUEUE, 55);
        try {
            runner().withPropertyValues(
                            "solace.request-reply.reply.provision-mode=CREATE_IF_MISSING",
                            "solace.request-reply.reply.quota-mb=999")
                    .run(context -> assertThat(context).hasFailed()
                            .getFailure()
                            .as("drift must say which property and why, not just that something broke")
                            .hasMessageContaining("drifted")
                            .hasMessageContaining(QUEUE));
        } finally {
            deprovisionRaw(QUEUE);
        }
    }

    private static void provisionRaw(String queueName, int quotaMb) throws Exception {
        Queue queue = JCSMPFactory.onlyInstance().createQueue(queueName);
        EndpointProperties props = new EndpointProperties();
        props.setPermission(EndpointProperties.PERMISSION_CONSUME);
        props.setAccessType(EndpointProperties.ACCESSTYPE_EXCLUSIVE);
        props.setQuota(quotaMb);
        props.setRespectsMsgTTL(Boolean.TRUE);
        rawSession.provision(queue, props,
                JCSMPSession.FLAG_IGNORE_ALREADY_EXISTS | JCSMPSession.WAIT_FOR_CONFIRM);
    }

    private static void deprovisionRaw(String queueName) {
        try {
            Queue queue = JCSMPFactory.onlyInstance().createQueue(queueName);
            rawSession.deprovision(queue, JCSMPSession.FLAG_IGNORE_DOES_NOT_EXIST);
        } catch (Exception ignored) {
            // Nothing to remove.
        }
    }
}
