package com.solace.samples.requestreply.integration;

import com.solace.samples.requestreply.support.SolaceTestBroker;
import com.solacesystems.jcsmp.EndpointProperties;
import com.solacesystems.jcsmp.JCSMPErrorResponseException;
import com.solacesystems.jcsmp.JCSMPErrorResponseSubcodeEx;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.PropertyMismatchException;
import com.solacesystems.jcsmp.Queue;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Locks in the broker behaviour that makes {@code CREATE_IF_MISSING} safe to ship enabled.
 *
 * <p>The design rests on one claim: {@code FLAG_IGNORE_ALREADY_EXISTS} tolerates an existing queue
 * but <em>not</em> one whose properties differ. If that were wrong — if a mismatch were silently
 * accepted — configuration could drift away from reality with no signal, and the default would be
 * unsafe. This is a behaviour of the broker and the API, not of our code, so it deserves a test
 * that fails loudly if a future version changes it.
 */
class ProvisionDriftIntegrationTest {

    private static JCSMPSession session;
    private static final String QUEUE = "q.test.provision.drift";

    @BeforeAll
    static void connect() throws Exception {
        JCSMPProperties p = new JCSMPProperties();
        p.setProperty(JCSMPProperties.HOST, SolaceTestBroker.smfHost());
        p.setProperty(JCSMPProperties.VPN_NAME, SolaceTestBroker.vpn());
        p.setProperty(JCSMPProperties.USERNAME, SolaceTestBroker.username());
        p.setProperty(JCSMPProperties.PASSWORD, SolaceTestBroker.password());
        session = JCSMPFactory.onlyInstance().createSession(p);
        session.connect();
    }

    @AfterAll
    static void disconnect() {
        if (session != null) { session.closeSession(); }
    }

    @Test
    void identicalReprovisionIsIdempotentButDriftIsRejected() throws Exception {
        Queue queue = JCSMPFactory.onlyInstance().createQueue(QUEUE);
        try {
            session.deprovision(queue, JCSMPSession.FLAG_IGNORE_DOES_NOT_EXIST);
        } catch (Exception ignored) {
            // Nothing to remove on a first run.
        }

        provision(queue, 100, 3);

        assertThatCode(() -> provision(queue, 100, 3))
                .as("re-provisioning with identical properties must be a no-op, otherwise "
                        + "CREATE_IF_MISSING could not be used on every startup")
                .doesNotThrowAnyException();

        assertThatThrownBy(() -> provision(queue, 200, 3))
                .as("a quota that differs must be rejected, not silently ignored")
                .isInstanceOf(PropertyMismatchException.class)
                .satisfies(e -> {
                    PropertyMismatchException pm = (PropertyMismatchException) e;
                    // The property name and the broker's value are what let CREATE_IF_MISSING
                    // report a useful diff with no SEMP call and no string parsing.
                    assertThat(pm.getProperty()).isNotBlank();
                    assertThat(pm.getPropertyValue()).isNotNull();
                });

        assertThatThrownBy(() -> provision(queue, 100, 9))
                .as("max-redelivery drift must be rejected too, not just quota")
                .isInstanceOf(PropertyMismatchException.class);

        session.deprovision(queue, JCSMPSession.FLAG_IGNORE_DOES_NOT_EXIST);
    }

    @Test
    void withoutTheIgnoreFlagAnExistingQueueIsAnError() throws Exception {
        Queue queue = JCSMPFactory.onlyInstance().createQueue(QUEUE + ".exists");
        try {
            session.deprovision(queue, JCSMPSession.FLAG_IGNORE_DOES_NOT_EXIST);
        } catch (Exception ignored) {
            // Nothing to remove.
        }
        provision(queue, 100, 3);

        assertThatThrownBy(() -> {
            EndpointProperties props = endpointProperties(100, 3);
            session.provision(queue, props, JCSMPSession.WAIT_FOR_CONFIRM);
        })
                .as("this is the condition the ignore flag suppresses, and it is distinct from "
                        + "property mismatch")
                .isInstanceOf(JCSMPErrorResponseException.class)
                .satisfies(e -> assertThat(((JCSMPErrorResponseException) e).getSubcodeEx())
                        .isEqualTo(JCSMPErrorResponseSubcodeEx.ENDPOINT_ALREADY_EXISTS));

        session.deprovision(queue, JCSMPSession.FLAG_IGNORE_DOES_NOT_EXIST);
    }

    private static void provision(Queue queue, int quotaMb, int maxRedelivery) throws Exception {
        session.provision(queue, endpointProperties(quotaMb, maxRedelivery),
                JCSMPSession.FLAG_IGNORE_ALREADY_EXISTS | JCSMPSession.WAIT_FOR_CONFIRM);
    }

    private static EndpointProperties endpointProperties(int quotaMb, int maxRedelivery) {
        EndpointProperties props = new EndpointProperties();
        props.setPermission(EndpointProperties.PERMISSION_CONSUME);
        props.setAccessType(EndpointProperties.ACCESSTYPE_NONEXCLUSIVE);
        props.setQuota(quotaMb);
        props.setMaxMsgRedelivery(maxRedelivery);
        props.setRespectsMsgTTL(Boolean.TRUE);
        return props;
    }
}
