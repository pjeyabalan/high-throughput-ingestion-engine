package com.enterprise.ingestion.core;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.LongAdder;

/**
 * High-Throughput Zero-Contention Prometheus Metrics Registry.
 *
 * Engineered for Enterprise Gateways & FDE Ingestion Pipelines:
 * 1. Backed by LongAdder: High-concurrency increments occur in independent per-core memory cells,
 *    eliminating CPU cache-line bouncing (unlike AtomicLong).
 * 2. Zero Runtime Dependency: Emits standard OpenMetrics / Prometheus exposition text format
 *    without requiring external Micrometer or Spring Boot Actuator libraries.
 * 3. Constant O(1) Memory Footprint: Dynamically registers counters per label dimension
 *    using ConcurrentHashMap with computeIfAbsent.
 */
public class PrometheusMetricsRegistry {

    private static final Logger log = System.getLogger(PrometheusMetricsRegistry.class.getName());

    // Counter metrics partitioned by "metricName{labels}"
    private final ConcurrentMap<String, LongAdder> counters = new ConcurrentHashMap<>();

    // Gauge suppliers for queue depth and available tokens
    private final ConcurrentMap<String, java.util.function.LongSupplier> gauges = new ConcurrentHashMap<>();

    public PrometheusMetricsRegistry() {
        log.log(Level.INFO, "Initialized PrometheusMetricsRegistry with lock-free LongAdder counters");
    }

    /**
     * Increments a named counter with specified label dimensions.
     * High-speed hot path: zero lock contention.
     *
     * @param metricName Name of the Prometheus metric (e.g., "llm_requests_total")
     * @param labelKey Label name (e.g., "status")
     * @param labelValue Label value (e.g., "processed", "throttled")
     */
    public void increment(String metricName, String labelKey, String labelValue) {
        String key = metricName + "{" + labelKey + "=\"" + labelValue + "\"}";
        counters.computeIfAbsent(key, k -> new LongAdder()).increment();
    }

    /**
     * Adds an arbitrary positive delta to a named counter.
     */
    public void add(String metricName, String labelKey, String labelValue, long delta) {
        if (delta > 0) {
            String key = metricName + "{" + labelKey + "=\"" + labelValue + "\"}";
            counters.computeIfAbsent(key, k -> new LongAdder()).add(delta);
        }
    }

    /**
     * Registers a live gauge callback (e.g. queue depth, heap memory).
     */
    public void registerGauge(String metricName, java.util.function.LongSupplier supplier) {
        gauges.put(metricName, supplier);
    }

    /**
     * Reads a specific counter value (used for assertions and health probes).
     */
    public long getCount(String metricName, String labelKey, String labelValue) {
        String key = metricName + "{" + labelKey + "=\"" + labelValue + "\"}";
        LongAdder adder = counters.get(key);
        return adder != null ? adder.sum() : 0L;
    }

    /**
     * Renders standard Prometheus / OpenMetrics text representation for /metrics endpoint.
     * Cold path: executed only once every 15-30s during Prometheus scrape.
     */
    public String scrape() {
        StringBuilder sb = new StringBuilder(1024);

        // Render Gauges
        for (var entry : gauges.entrySet()) {
            String name = entry.getKey();
            long value = entry.getValue().getAsLong();
            sb.append("# TYPE ").append(name).append(" gauge\n");
            sb.append(name).append(" ").append(value).append("\n");
        }

        // Render Counters
        sb.append("# TYPE ingestion_events_total counter\n");
        for (var entry : counters.entrySet()) {
            sb.append(entry.getKey()).append(" ").append(entry.getValue().sum()).append("\n");
        }

        return sb.toString();
    }
}
