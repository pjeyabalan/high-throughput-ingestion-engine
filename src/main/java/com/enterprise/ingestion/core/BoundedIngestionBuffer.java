package com.enterprise.ingestion.core;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;

/**
 * High-Throughput Bounded Ingestion Buffer.
 *
 * Design Highlights:
 * 1. Backed by ArrayBlockingQueue: Pre-allocates backing array, eliminating
 *    per-node object churn in Eden space (unlike LinkedBlockingQueue).
 * 2. Strict Backpressure: Producers must block or timeout if capacity is exhausted.
 * 3. Lock-free metric tracking via LongAdder across high-contention worker threads.
 * 4. Graceful draining shutdown: All in-flight queue items are processed before termination.
 * 5. Zero-dependency logging via Java Platform Logging (System.Logger), auto-routing to SLF4J if present.
 *
 * @param <T> Item type
 */
public class BoundedIngestionBuffer<T> implements AutoCloseable {

    private static final Logger log = System.getLogger(BoundedIngestionBuffer.class.getName());

    private final int capacity;
    private final BlockingQueue<T> queue;
    private final ExecutorService consumerPool;
    private final ConsumerProcessor<T> processor;

    private final AtomicBoolean isRunning = new AtomicBoolean(true);
    private final LongAdder enqueuedCount = new LongAdder();
    private final LongAdder processedCount = new LongAdder();
    private final LongAdder droppedCount = new LongAdder();
    private final LongAdder failureCount = new LongAdder();

    public BoundedIngestionBuffer(int capacity, int consumerThreads, ConsumerProcessor<T> processor) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("Capacity must be strictly positive");
        }
        if (consumerThreads <= 0) {
            throw new IllegalArgumentException("consumerThreads must be strictly positive");
        }
        this.capacity = capacity;
        this.processor = Objects.requireNonNull(processor, "ConsumerProcessor cannot be null");

        // ArrayBlockingQueue: contiguous memory, zero dynamic node allocation
        this.queue = new ArrayBlockingQueue<>(capacity);

        ThreadFactory factory = new NamedThreadFactory("ingest-consumer");
        this.consumerPool = Executors.newFixedThreadPool(consumerThreads, factory);

        // Spawn consumer daemon loops
        for (int i = 0; i < consumerThreads; i++) {
            consumerPool.submit(this::consumerLoop);
        }

        log.log(Level.INFO, "Initialized BoundedIngestionBuffer [capacity={0}, consumerThreads={1}]", capacity, consumerThreads);
    }

    /**
     * Attempts to offer an item into the buffer with a backpressure timeout.
     *
     * @param item Item to ingest
     * @param timeout Maximum wait time if queue is full
     * @param unit Time unit for timeout
     * @return true if accepted, false if rejected due to timeout (backpressure triggered)
     * @throws InterruptedException If the offering thread is interrupted
     */
    public boolean offer(T item, long timeout, TimeUnit unit) throws InterruptedException {
        Objects.requireNonNull(item, "Cannot ingest null item");

        if (!isRunning.get()) {
            throw new IllegalStateException("Ingestion buffer has been shut down");
        }

        boolean accepted = queue.offer(item, timeout, unit);
        if (accepted) {
            enqueuedCount.increment();
        } else {
            droppedCount.increment();
            log.log(Level.WARNING, "Ingestion backpressure triggered: Queue at capacity ({0}/{1}). Item rejected.", 
                    queue.size(), capacity);
        }
        return accepted;
    }

    /**
     * Consumer loop: pulls items from queue and invokes processor callback.
     */
    private void consumerLoop() {
        while (isRunning.get() || !queue.isEmpty()) {
            try {
                // Poll with timeout to periodically re-check isRunning flag
                T item = queue.poll(100, TimeUnit.MILLISECONDS);
                if (item != null) {
                    try {
                        processor.process(item);
                        processedCount.increment();
                    } catch (Exception ex) {
                        failureCount.increment();
                        log.log(Level.ERROR, "Failed to process ingested item", ex);
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.log(Level.INFO, "Consumer thread interrupted. Preparing to exit.");
                break;
            }
        }
    }

    /**
     * Initiates a graceful shutdown, allowing all remaining queued items to drain.
     *
     * @param timeout Duration to wait for consumers to finish draining
     * @param unit TimeUnit
     * @return true if cleanly terminated, false if timeout elapsed before queue drained
     * @throws InterruptedException If awaiting thread is interrupted
     */
    public boolean shutdownAndDrain(long timeout, TimeUnit unit) throws InterruptedException {
        log.log(Level.INFO, "Initiating graceful shutdown. Remaining items to drain: {0}", queue.size());
        isRunning.set(false);
        consumerPool.shutdown();

        boolean completed = consumerPool.awaitTermination(timeout, unit);
        if (!completed) {
            log.log(Level.WARNING, "Consumer pool did not terminate within timeout. Forcing shutdownNow().");
            consumerPool.shutdownNow();
        } else {
            log.log(Level.INFO, "Graceful shutdown complete. Total processed: {0}", processedCount.sum());
        }
        return completed;
    }

    @Override
    public void close() throws Exception {
        shutdownAndDrain(10, TimeUnit.SECONDS);
    }

    // Metric accessors
    public int getQueueDepth() { return queue.size(); }
    public int getCapacity() { return capacity; }
    public long getEnqueuedCount() { return enqueuedCount.sum(); }
    public long getProcessedCount() { return processedCount.sum(); }
    public long getDroppedCount() { return droppedCount.sum(); }
    public long getFailureCount() { return failureCount.sum(); }
    public boolean isRunning() { return isRunning.get(); }
}
