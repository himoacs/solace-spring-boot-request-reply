package com.solace.samples.requestreply.config;

import com.solace.samples.requestreply.config.SolaceRequestReplyProperties.Backpressure;
import com.solace.samples.requestreply.config.SolaceRequestReplyProperties.Backpressure.Resolved;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Pure logic, no Spring context needed: {@link Backpressure#resolve} against a listener's concurrency. */
class SolaceRequestReplyPropertiesBackpressureTest {

    @Test
    void allAutoValuesResolveAgainstConcurrency() {
        Resolved r = new Backpressure().resolve(4);

        assertThat(r.enabled()).isTrue();
        assertThat(r.queueCapacity()).isEqualTo(8);       // 2 * concurrency
        assertThat(r.pauseAtQueueDepth()).isEqualTo(8);   // pause once the buffer is full
        assertThat(r.resumeAtQueueDepth()).isEqualTo(4);  // half of capacity
    }

    @Test
    void explicitValuesOverrideAuto() {
        Backpressure bp = new Backpressure();
        bp.setQueueCapacity(100);
        bp.setPauseAtQueueDepth(80);
        bp.setResumeAtQueueDepth(20);

        Resolved r = bp.resolve(4);

        assertThat(r.queueCapacity()).isEqualTo(100);
        assertThat(r.pauseAtQueueDepth()).isEqualTo(80);
        assertThat(r.resumeAtQueueDepth()).isEqualTo(20);
    }

    @Test
    void pauseAndResumeAreClampedToACoherentOrdering() {
        Backpressure bp = new Backpressure();
        bp.setQueueCapacity(10);
        bp.setPauseAtQueueDepth(50);  // above capacity
        bp.setResumeAtQueueDepth(50); // above the (clamped) pause point

        Resolved r = bp.resolve(4);

        assertThat(r.pauseAtQueueDepth()).isEqualTo(10);  // clamped to capacity
        assertThat(r.resumeAtQueueDepth()).isEqualTo(10); // clamped to pauseAtQueueDepth
    }

    @Test
    void oneConcurrencyStillYieldsAUsableQueue() {
        Resolved r = new Backpressure().resolve(1);

        assertThat(r.queueCapacity()).isEqualTo(2);
        assertThat(r.resumeAtQueueDepth()).isGreaterThanOrEqualTo(1);
    }

    @Test
    void disabledIsPreservedThroughResolution() {
        Backpressure bp = new Backpressure();
        bp.setEnabled(false);

        assertThat(bp.resolve(4).enabled()).isFalse();
    }
}
