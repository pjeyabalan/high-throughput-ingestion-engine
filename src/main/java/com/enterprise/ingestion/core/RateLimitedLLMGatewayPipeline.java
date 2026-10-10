package com.enterprise.ingestion.core;

import com.enterprise.ingestion.model.IngestionEvent;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;

/**
 * Resilient LLM Gateway Ingestion Pipeline with Integrated Prometheus Metrics.
 *
 * Demonstrates:
 * 1. Token-Bucket Rate Limiter protecting downstream LLM endpoints from 429 RateLimit errors.
 * 2. ThreadPoolExecutor configured with CallerRunsPolicy: When internal queue is saturated,
 *    the calling producer thread executes the task itself, applying natural backpressure.
 * 3. Lock-free PrometheusMetricsRegistry recording real-time throughput and queue depth.
 */
public class RateLimitedLLMGatewayPipeline implements AutoCloseable {

    private static final Logger log = System.getLogger(RateLimitedLLMGatewayPipeline.class.getName());

    private final TokenBucketRateLimiter rateLimiter;
    private final BlockingQueue<Runnable> taskQueue;
    private final ThreadPoolExecutor workerPool;
    private final ConsumerProcessor<IngestionEvent> llmProcessor;
    private final PrometheusMetricsRegistry metricsRegistry;

    private final AtomicBoolean isRunning = new AtomicBoolean(true);
    private final LongAdder requestsDispatched = new LongAdder();
    private final LongAdder throttledCount = new LongAdder();
    private final LongAdder completedCount = new LongAdder();

    public RateLimitedLLMGatewayPipeline(
            int queueCapacity,
            int workerThreads,
            double maxTokens,
            double refillTokensPerSec,
            ConsumerProcessor<IngestionEvent> llmProcessor
    ) {
        this(queueCapacity, workerThreads, maxTokens, refillTokensPerSec, llmProcessor, new PrometheusMetricsRegistry());
    }

    public RateLimitedLLMGatewayPipeline(
            int queueCapacity,
            int workerThreads,
            double maxTokens,
            double refillTokensPerSec,
            ConsumerProcessor<IngestionEvent> llmProcessor,
            PrometheusMetricsRegistry metricsRegistry
    ) {
        this.rateLimiter = new TokenBucketRateLimiter(maxTokens, refillTokensPerSec);
        this.llmProcessor = Objects.requireNonNull(llmProcessor, "llmProcessor cannot be null");
        this.metricsRegistry = Objects.requireNonNull(metricsRegistry, "metricsRegistry cannot be null");
        this.taskQueue = new ArrayBlockingQueue<>(queueCapacity);

        // Register live Prometheus gauges
        this.metricsRegistry.registerGauge("llm_gateway_queue_depth", taskQueue::size);
        this.metricsRegistry.registerGauge("llm_gateway_available_tokens", () -> (long) rateLimiter.getAvailableTokens());

        ThreadFactory threadFactory = new NamedThreadFactory("llm-gateway-worker");

        // CallerRunsPolicy: when queue is full, the calling thread runs the task itself!
        // This throttles the upstream ingress controller naturally.
        this.workerPool = new ThreadPoolExecutor(
                workerThreads,
                workerThreads,
                0L, TimeUnit.MILLISECONDS,
                this.taskQueue,
                threadFactory,
                new ThreadPoolExecutor.CallerRunsPolicy()
        );

        log.log(Level.INFO, "Initialized RateLimitedLLMGatewayPipeline with CallerRunsPolicy and Prometheus telemetry");
    }

    /**
     * Submits an event to the LLM Gateway.
     * Enforces token rate limits and bounded queue backpressure.
     *
     * @param event The ingestion/LLM query event
     * @param estimatedTokens Tokens consumed by this prompt/payload
     * @param timeout Maximum wait time for rate-limiter tokens
     * @param unit TimeUnit
     * @return true if successfully accepted into pipeline, false if throttled/rejected
     * @throws InterruptedException If thread is interrupted
     */
    public boolean submit(IngestionEvent event, int estimatedTokens, long timeout, TimeUnit unit) throws InterruptedException {
        if (!isRunning.get()) {
            throw new IllegalStateException("Gateway is shut down");
        }

        // 1. Acquire tokens from TokenBucket (protects LLM TPM/RPM)
        boolean tokenAcquired = rateLimiter.acquire(estimatedTokens, timeout, unit);
        if (!tokenAcquired) {
            throttledCount.increment();
            metricsRegistry.increment("llm_gateway_requests_total", "status", "throttled");
            log.log(Level.WARNING, "LLM Gateway: Query {0} throttled due to token rate limits", event.eventId());
            return false;
        }

        // 2. Dispatch task to worker pool (CallerRunsPolicy kicks in if queue is full)
        workerPool.execute(() -> {
            try {
                requestsDispatched.increment();
                metricsRegistry.increment("llm_gateway_requests_total", "status", "dispatched");
                llmProcessor.process(event);
                completedCount.increment();
                metricsRegistry.increment("llm_gateway_requests_total", "status", "completed");
            } catch (Exception ex) {
                metricsRegistry.increment("llm_gateway_requests_total", "status", "failed");
                log.log(Level.ERROR, "Error executing LLM request for event {0}", event.eventId(), ex);
            }
        });

        return true;
    }

    public boolean shutdownAndAwait(long timeout, TimeUnit unit) throws InterruptedException {
        isRunning.set(false);
        workerPool.shutdown();
        return workerPool.awaitTermination(timeout, unit);
    }

    @Override
    public void close() throws Exception {
        shutdownAndAwait(5, TimeUnit.SECONDS);
    }

    public PrometheusMetricsRegistry getMetricsRegistry() { return metricsRegistry; }
    public long getRequestsDispatched() { return requestsDispatched.sum(); }
    public long getThrottledCount() { return throttledCount.sum(); }
    public long getCompletedCount() { return completedCount.sum(); }
    public int getQueueDepth() { return taskQueue.size(); }
}
