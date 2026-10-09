package com.enterprise.ingestion.core;

import com.enterprise.ingestion.model.IngestionEvent;

import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

/**
 * Standalone Zero-Dependency Test & Stress Runner for Day 01.
 *
 * Verifies:
 * 1. Concurrent ingestion throughput across multiple producer threads.
 * 2. Backpressure rejection when queue capacity is reached.
 * 3. Graceful draining on shutdown with zero event loss.
 */
public class IngestionSmokeTestRunner {

    public static void main(String[] args) throws Exception {
        System.out.println("=============================================================");
        System.out.println(" DAY 01: BOUNDED INGESTION BUFFER VERIFICATION & STRESS TEST");
        System.out.println("=============================================================");

        testConcurrentIngestion(10_000, 4, 1000);
        testBackpressureRejection();
        testGracefulShutdownDraining(500);

        System.out.println("\n>>> ALL DAY 01 INGESTION ENGINE TESTS PASSED CLEANLY! <<<");
    }

    private static void testConcurrentIngestion(int totalEvents, int consumerThreads, int queueCapacity) throws Exception {
        System.out.println("\n[Test 1] Concurrently ingesting " + totalEvents + " events...");
        CountDownLatch latch = new CountDownLatch(totalEvents);
        LongAdder processedCounter = new LongAdder();

        long start = System.currentTimeMillis();
        try (BoundedIngestionBuffer<IngestionEvent> buffer = new BoundedIngestionBuffer<>(
                queueCapacity, consumerThreads, event -> {
            processedCounter.increment();
            latch.countDown();
        })) {
            for (int i = 0; i < totalEvents; i++) {
                IngestionEvent event = new IngestionEvent(
                        "evt-" + i, "tenant-alpha", "TRANSACTION", 256, Instant.now()
                );
                boolean accepted = buffer.offer(event, 5, TimeUnit.SECONDS);
                if (!accepted) {
                    throw new IllegalStateException("Unexpected drop during test: " + i);
                }
            }

            boolean completed = latch.await(10, TimeUnit.SECONDS);
            long duration = System.currentTimeMillis() - start;

            if (!completed) {
                throw new AssertionError("Test timed out before processing all events");
            }
            System.out.printf("  ✓ Processed %d events in %d ms (Throughput: %,d ops/sec)%n",
                    processedCounter.sum(), duration, (totalEvents * 1000L / Math.max(1, duration)));
        }
    }

    private static void testBackpressureRejection() throws Exception {
        System.out.println("\n[Test 2] Verifying backpressure rejection on buffer saturation...");
        int capacity = 5;
        CountDownLatch holdLatch = new CountDownLatch(1);

        try (BoundedIngestionBuffer<IngestionEvent> buffer = new BoundedIngestionBuffer<>(
                capacity, 1, event -> holdLatch.await()
        )) {
            // Settle capacity: 1 is picked up by the blocked consumer, capacity (5) fill the queue
            for (int i = 0; i <= capacity; i++) {
                boolean accepted = buffer.offer(
                        new IngestionEvent("e-" + i, "tenant-test", "AUDIT", 128, Instant.now()),
                        100, TimeUnit.MILLISECONDS
                );
                if (!accepted) {
                    holdLatch.countDown();
                    throw new AssertionError("Failed to fill capacity at item " + i);
                }
            }

            // Next offer must trigger backpressure rejection
            boolean rejected = buffer.offer(
                    new IngestionEvent("overflow-item", "tenant-test", "AUDIT", 128, Instant.now()),
                    50, TimeUnit.MILLISECONDS
            );

            holdLatch.countDown(); // Release consumer

            if (rejected) {
                throw new AssertionError("Expected item to be rejected, but it was accepted!");
            }

            System.out.println("  ✓ Backpressure successfully triggered! Dropped count: " + buffer.getDroppedCount());
        }
    }

    private static void testGracefulShutdownDraining(int count) throws Exception {
        System.out.println("\n[Test 3] Verifying zero-data-loss graceful shutdown draining...");
        LongAdder drainCounter = new LongAdder();

        BoundedIngestionBuffer<IngestionEvent> buffer = new BoundedIngestionBuffer<>(
                1000, 2, event -> {
            Thread.sleep(1);
            drainCounter.increment();
        });

        for (int i = 0; i < count; i++) {
            buffer.offer(new IngestionEvent("e-" + i, "tenant-drain", "FLUSH", 64, Instant.now()),
                    1, TimeUnit.SECONDS);
        }

        boolean drained = buffer.shutdownAndDrain(5, TimeUnit.SECONDS);
        if (!drained || drainCounter.sum() != count) {
            throw new AssertionError("Data loss during shutdown! Processed: " + drainCounter.sum() + ", Expected: " + count);
        }

        System.out.printf("  ✓ Flushed all %d items cleanly with zero queue retention.%n", drainCounter.sum());
    }
}
