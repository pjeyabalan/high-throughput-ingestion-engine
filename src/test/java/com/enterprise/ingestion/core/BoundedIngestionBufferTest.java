package com.enterprise.ingestion.core;

import com.enterprise.ingestion.model.IngestionEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BoundedIngestionBufferTest {

    @Test
    @DisplayName("Should successfully enqueue and process events concurrently")
    void shouldProcessEventsConcurrently() throws Exception {
        int eventCount = 1000;
        CountDownLatch latch = new CountDownLatch(eventCount);
        LongAdder processedCounter = new LongAdder();

        try (BoundedIngestionBuffer<IngestionEvent> buffer = new BoundedIngestionBuffer<>(
                200, 4, event -> {
            processedCounter.increment();
            latch.countDown();
        })) {
            for (int i = 0; i < eventCount; i++) {
                IngestionEvent event = new IngestionEvent(
                        "evt-" + i, "tenant-101", "TRANSACTION", 256, Instant.now()
                );
                boolean accepted = buffer.offer(event, 5, TimeUnit.SECONDS);
                assertThat(accepted).isTrue();
            }

            boolean completed = latch.await(10, TimeUnit.SECONDS);
            assertThat(completed).isTrue();
            assertThat(processedCounter.sum()).isEqualTo(eventCount);
            assertThat(buffer.getEnqueuedCount()).isEqualTo(eventCount);
        }
    }

    @Test
    @DisplayName("Should trigger backpressure rejection when buffer capacity is saturated")
    void shouldRejectWhenCapacityExceeded() throws Exception {
        int smallCapacity = 5;
        CountDownLatch holdLatch = new CountDownLatch(1);

        // Slow consumer that blocks until released
        try (BoundedIngestionBuffer<IngestionEvent> buffer = new BoundedIngestionBuffer<>(
                smallCapacity, 1, event -> holdLatch.await()
        )) {
            // Fill capacity
            for (int i = 0; i < smallCapacity; i++) {
                boolean accepted = buffer.offer(
                        new IngestionEvent("e-" + i, "t1", "TEST", 100, Instant.now()),
                        50, TimeUnit.MILLISECONDS
                );
                assertThat(accepted).isTrue();
            }

            // Next offer must time out and fail
            boolean rejected = buffer.offer(
                    new IngestionEvent("e-overflow", "t1", "TEST", 100, Instant.now()),
                    50, TimeUnit.MILLISECONDS
            );
            assertThat(rejected).isFalse();
            assertThat(buffer.getDroppedCount()).isGreaterThanOrEqualTo(1);

            // Release consumer so buffer can close cleanly
            holdLatch.countDown();
        }
    }

    @Test
    @DisplayName("Should cleanly drain all queued events upon graceful shutdown")
    void shouldDrainAllEventsOnShutdown() throws Exception {
        int totalEvents = 500;
        AtomicInteger processed = new AtomicInteger(0);

        BoundedIngestionBuffer<IngestionEvent> buffer = new BoundedIngestionBuffer<>(
                1000, 2, event -> {
            Thread.sleep(2); // Small artificial processing delay
            processed.incrementAndGet();
        });

        for (int i = 0; i < totalEvents; i++) {
            buffer.offer(
                    new IngestionEvent("evt-" + i, "tenant-alpha", "PAYMENT", 512, Instant.now()),
                    1, TimeUnit.SECONDS
            );
        }

        // Trigger shutdown and drain
        boolean drainedCleanly = buffer.shutdownAndDrain(5, TimeUnit.SECONDS);

        assertThat(drainedCleanly).isTrue();
        assertThat(processed.get()).isEqualTo(totalEvents);
        assertThat(buffer.getQueueDepth()).isZero();
    }

    @Test
    @DisplayName("Should reject invalid construction arguments")
    void shouldRejectInvalidArguments() {
        assertThatThrownBy(() -> new BoundedIngestionBuffer<>(0, 2, item -> {}))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new BoundedIngestionBuffer<>(100, 0, item -> {}))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new BoundedIngestionBuffer<>(100, 2, null))
                .isInstanceOf(NullPointerException.class);
    }
}
