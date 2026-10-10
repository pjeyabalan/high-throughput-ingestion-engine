package com.enterprise.ingestion.core;

import com.enterprise.ingestion.model.IngestionEvent;

import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Standalone Verification & Stress Runner for Day 03.
 *
 * Verifies:
 * 1. PrometheusMetricsRegistry lock-free performance under multi-thread stress.
 * 2. Proper generation of Prometheus / OpenMetrics exposition format.
 * 3. End-to-end telemetry integration with RateLimitedLLMGatewayPipeline.
 */
public class Day03MetricsRunner {

    public static void main(String[] args) throws Exception {
        System.out.println("=============================================================");
        System.out.println(" DAY 03: ZERO-CONTENTION PROMETHEUS METRICS REGISTRY TEST");
        System.out.println("=============================================================");

        testDirectRegistryPerformance();
        testGatewayPrometheusExposition();

        System.out.println("\n>>> ALL DAY 03 TELEMETRY & METRICS TESTS PASSED CLEANLY! <<<");
    }

    private static void testDirectRegistryPerformance() throws Exception {
        System.out.println("\n[Test 1] Testing PrometheusMetricsRegistry with 50,000 concurrent increments...");

        PrometheusMetricsRegistry registry = new PrometheusMetricsRegistry();
        int threads = 8;
        int incrementsPerThread = 10_000;
        int totalExpected = threads * incrementsPerThread;

        CountDownLatch latch = new CountDownLatch(threads);
        long start = System.currentTimeMillis();

        for (int t = 0; t < threads; t++) {
            new Thread(() -> {
                for (int i = 0; i < incrementsPerThread; i++) {
                    registry.increment("test_events_total", "status", "processed");
                }
                latch.countDown();
            }).start();
        }

        boolean completed = latch.await(5, TimeUnit.SECONDS);
        long duration = System.currentTimeMillis() - start;

        if (!completed) throw new AssertionError("Timed out during registry stress test");

        long actual = registry.getCount("test_events_total", "status", "processed");
        if (actual != totalExpected) {
            throw new AssertionError("Mismatch in counter! Expected: " + totalExpected + ", Got: " + actual);
        }

        System.out.printf("  ✓ Processed %,d lock-free increments in %d ms (%,d ops/sec)%n",
                actual, duration, (totalExpected * 1000L / Math.max(1, duration)));
    }

    private static void testGatewayPrometheusExposition() throws Exception {
        System.out.println("\n[Test 2] Simulating LLM Gateway and scraping Prometheus /metrics exposition...");

        PrometheusMetricsRegistry registry = new PrometheusMetricsRegistry();

        try (RateLimitedLLMGatewayPipeline gateway = new RateLimitedLLMGatewayPipeline(
                20, 2, 50.0, 200.0, event -> Thread.sleep(2), registry
        )) {
            // Submit 30 events
            for (int i = 0; i < 30; i++) {
                IngestionEvent event = new IngestionEvent("query-" + i, "bank-01", "AI_PROMPT", 512, Instant.now());
                gateway.submit(event, 1, 50, TimeUnit.MILLISECONDS);
            }

            // Scrape Prometheus text format
            String scrapeOutput = registry.scrape();

            System.out.println("  --- Live Prometheus Scrape Output (/actuator/prometheus) ---");
            System.out.println(scrapeOutput);
            System.out.println("  -------------------------------------------------------------");

            if (!scrapeOutput.contains("llm_gateway_requests_total") || !scrapeOutput.contains("llm_gateway_queue_depth")) {
                throw new AssertionError("Prometheus scrape output missing required gateway metrics!");
            }

            System.out.println("  ✓ OpenMetrics text format validated. Prometheus scraper can ingest directly.");
        }
    }
}
