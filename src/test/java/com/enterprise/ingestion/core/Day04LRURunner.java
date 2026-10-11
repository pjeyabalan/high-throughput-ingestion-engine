package com.enterprise.ingestion.core;

import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Verification & Concurrency Stress Runner for Thread-Safe LRUCache.
 */
public class Day04LRURunner {

    public static void main(String[] args) throws Exception {
        System.out.println("=============================================================");
        System.out.println(" DAY 04: THREAD-SAFE O(1) LRU CACHE VERIFICATION & STRESS TEST");
        System.out.println("=============================================================");

        testLRUEvictionLogic();
        testRecencyUpdateOnGet();
        testConcurrentMultiThreadedStress();

        System.out.println("\n>>> ALL DAY 04 LRU CACHE TESTS PASSED CLEANLY! <<<");
    }

    private static void testLRUEvictionLogic() {
        System.out.println("\n[Test 1] Testing basic O(1) put and LRU eviction order...");
        LRUCache<String, String> cache = new LRUCache<>(3);

        cache.put("k1", "v1");
        cache.put("k2", "v2");
        cache.put("k3", "v3");

        if (cache.size() != 3) throw new AssertionError("Expected size 3");

        // Inserting 4th element must evict least recently used ("k1")
        cache.put("k4", "v4");

        if (cache.containsKey("k1")) throw new AssertionError("k1 should have been evicted!");
        if (!cache.containsKey("k2") || !cache.containsKey("k3") || !cache.containsKey("k4")) {
            throw new AssertionError("k2, k3, k4 must be present!");
        }
        if (cache.getEvictionCount() != 1) {
            throw new AssertionError("Expected eviction count to be 1, got: " + cache.getEvictionCount());
        }

        System.out.println("  ✓ Correctly evicted oldest item ('k1') when capacity exceeded.");
    }

    private static void testRecencyUpdateOnGet() {
        System.out.println("\n[Test 2] Testing recency refresh on get() access...");
        LRUCache<String, Integer> cache = new LRUCache<>(3);

        cache.put("A", 100);
        cache.put("B", 200);
        cache.put("C", 300);

        // Access "A", making it the Most Recently Used (MRU)
        Optional<Integer> valA = cache.get("A");
        if (valA.isEmpty() || valA.get() != 100) throw new AssertionError("Failed to get A");

        // Now insert "D". The LRU item is now "B", so "B" must be evicted, NOT "A"!
        cache.put("D", 400);

        if (cache.containsKey("B")) throw new AssertionError("B should have been evicted as the LRU item!");
        if (!cache.containsKey("A")) throw new AssertionError("A should NOT be evicted because get(A) refreshed its recency!");
        if (!cache.containsKey("C") || !cache.containsKey("D")) throw new AssertionError("C and D must be present");

        System.out.println("  ✓ get('A') refreshed recency; item 'B' was evicted instead of 'A'.");
        System.out.printf("  ✓ Hit rate: %.1f%% (Hits: %d, Misses: %d)%n", 
                cache.getHitRate() * 100, cache.getHitCount(), cache.getMissCount());
    }

    private static void testConcurrentMultiThreadedStress() throws Exception {
        System.out.println("\n[Test 3] Testing multi-threaded concurrent put/get with 8 threads (50,000 ops)...");

        int capacity = 500;
        LRUCache<String, String> cache = new LRUCache<>(capacity);
        int threadCount = 8;
        int opsPerThread = 10_000;

        CountDownLatch latch = new CountDownLatch(threadCount);
        long start = System.currentTimeMillis();

        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            new Thread(() -> {
                for (int i = 0; i < opsPerThread; i++) {
                    String key = "key-" + (i % 800); // Intentionally create collisions and evictions
                    cache.put(key, "val-" + threadId + "-" + i);
                    cache.get(key);
                }
                latch.countDown();
            }).start();
        }

        boolean completed = latch.await(10, TimeUnit.SECONDS);
        long duration = System.currentTimeMillis() - start;

        if (!completed) throw new AssertionError("Stress test timed out");
        if (cache.size() > capacity) throw new AssertionError("Cache size exceeded capacity: " + cache.size());

        System.out.printf("  ✓ Processed %,d concurrent ops in %d ms (%,d ops/sec)%n",
                (threadCount * opsPerThread * 2), duration,
                ((long) threadCount * opsPerThread * 2 * 1000L / Math.max(1, duration)));
        System.out.printf("  ✓ Cache size safely capped at: %d (Evictions: %,d, Hits: %,d)%n",
                cache.size(), cache.getEvictionCount(), cache.getHitCount());
    }
}
