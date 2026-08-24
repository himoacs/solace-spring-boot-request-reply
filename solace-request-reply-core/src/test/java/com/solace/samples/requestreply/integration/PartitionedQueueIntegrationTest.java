package com.solace.samples.requestreply.integration;

import com.solace.samples.requestreply.config.SolaceRequestReplyProperties;
import com.solace.samples.requestreply.endpoint.SempClient;
import com.solace.samples.requestreply.exception.EndpointProvisioningException;
import com.solace.samples.requestreply.exception.PartitionMismatchException;
import com.solace.samples.requestreply.support.SolaceTestBroker;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Partition counts, which JCSMP cannot express and SEMP therefore owns.
 *
 * <p>Two behaviours are worth pinning down. Creating a partitioned queue is safe to automate,
 * because the queue is empty. <b>Changing</b> one is not: Solace requires draining first, and a
 * decrease deletes the messages held in removed partitions — so an application that patched this
 * on every startup could destroy queued bookings during a routine redeploy. A mismatch is
 * therefore fatal unless explicitly permitted.
 */
class PartitionedQueueIntegrationTest {

    private static final String QUEUE = "q.test.partitioned";

    private SempClient sempClient(int partitionCount, boolean allowResize) {
        SolaceRequestReplyProperties.Partitioning cfg =
                new SolaceRequestReplyProperties.Partitioning();
        cfg.setPartitionCount(partitionCount);
        cfg.setAllowPartitionResize(allowResize);
        cfg.getSemp().setUrl(SolaceTestBroker.sempUrl());
        cfg.getSemp().setUsername("admin");
        cfg.getSemp().setPassword("admin");
        cfg.getSemp().setMsgVpn(SolaceTestBroker.vpn());
        return new SempClient(cfg);
    }

    @Test
    void createsAPartitionedQueueAndAcceptsAMatchingCount() {
        SempClient client = sempClient(4, false);

        client.ensurePartitionedQueue(QUEUE, 4);
        assertThat(SolaceTestBroker.partitionCount(QUEUE))
                .as("JCSMP cannot set this, so SEMP must have")
                .isEqualTo(4);

        assertThatCode(() -> client.ensurePartitionedQueue(QUEUE, 4))
                .as("an unchanged count must be a no-op, so startup is repeatable")
                .doesNotThrowAnyException();
    }

    @Test
    void refusesToResizeUnlessExplicitlyAllowed() {
        SempClient create = sempClient(2, false);
        String queue = QUEUE + ".resize";
        create.ensurePartitionedQueue(queue, 2);

        assertThatThrownBy(() -> sempClient(8, false).ensurePartitionedQueue(queue, 8))
                .as("a silent resize could delete queued messages, so it must fail loudly")
                .isInstanceOf(PartitionMismatchException.class)
                .hasMessageContaining("allow-partition-resize");

        assertThatCode(() -> sempClient(8, true).ensurePartitionedQueue(queue, 8))
                .as("and must succeed once the operator has accepted that risk")
                .doesNotThrowAnyException();
        assertThat(SolaceTestBroker.partitionCount(queue)).isEqualTo(8);
    }

    @Test
    void explainsItselfWhenSempIsNotConfigured() {
        SolaceRequestReplyProperties.Partitioning cfg =
                new SolaceRequestReplyProperties.Partitioning();
        cfg.setPartitionCount(4);
        SempClient client = new SempClient(cfg);

        assertThatThrownBy(() -> client.ensurePartitionedQueue("q.test.nosemp", 4))
                .isInstanceOf(EndpointProvisioningException.class)
                // The message has to name the cause and the way out: "partitioning needs SEMP"
                // is the single least obvious constraint in the whole design.
                .hasMessageContaining("semp.url")
                .hasMessageContaining("partition-count=0");
    }
}
