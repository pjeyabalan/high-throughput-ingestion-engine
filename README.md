# High-Throughput Ingestion Engine & LLM Gateway

[![Java](https://img.shields.io/badge/Java-21-orange.svg)](https://openjdk.org/projects/jdk/21/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Build](https://img.shields.io/badge/Build-Maven-red.svg)](pom.xml)

Production-grade, low-latency ingestion framework engineered for enterprise data integration pipelines and LLM API Gateways. Built to ingest and process high-volume event streams under strict JVM memory boundaries without triggering `OutOfMemoryError: Java heap space` or downstream `HTTP 429: Rate Limit Exceeded` errors.

---

## 1. The Enterprise Problem

In high-concurrency enterprise pipelines (webhooks, IoT firehoses, LLM API gateways), naive asynchronous implementations fail under burst traffic:

```java
// ANTI-PATTERN: Fatal under burst loads
ExecutorService executor = Executors.newFixedThreadPool(16);
```
- **The Failure Vector**: `Executors.newFixedThreadPool` is backed by an **unbounded** `LinkedBlockingQueue` (`Integer.MAX_VALUE`).
- When downstream databases or external LLM APIs (Azure OpenAI, Anthropic, Bedrock) slow down or throttle with 429s, incoming tasks accumulate inside the queue.
- At 20k events/second, heap space exhausts within seconds, triggering `java.lang.OutOfMemoryError: Java heap space` or kernel `OOMKilled` (Exit code 137).

---

## 2. Architectural Solution

```
[Concurrent HTTP / LLM Query Producers]
                   |
                   v
    [TokenBucketRateLimiter] <--- Meters TPM / RPM (Nano-second lazy refill)
                   |
                   v
    [CallerRunsPolicy Backpressure] <--- Forces producer thread to run task if queue full
                   |
+-------------------------------------------------------------+
|         BOUNDED ARRAYBLOCKINGQUEUE (Pre-allocated)          |
|   [Event 1] [Event 2] [Event 3] ... [Event N (Capacity)]    |
+-------------------------------------------------------------+
                   |
                   +---> Worker Thread 1 (NamedThreadFactory)
                   +---> Worker Thread 2 (NamedThreadFactory)
                   +---> Worker Thread 3 (NamedThreadFactory)
                   |
                   v
    [Downstream Sink / Azure OpenAI / Anthropic API]
```

### Why `ArrayBlockingQueue` Over `LinkedBlockingQueue`
| Metric | `ArrayBlockingQueue` (Ours) | `LinkedBlockingQueue` |
| :--- | :--- | :--- |
| **Backing Structure** | Contiguous pre-allocated circular array | Singly-linked list of dynamic node objects |
| **Allocation Churn** | **0 objects** allocated during enqueue/dequeue | **1 Node object** allocated per enqueue (Eden churn) |
| **Memory Footprint** | Fixed, constant $O(1)$ bound | Variable, risk of unbounded heap growth |
| **CPU Cache Locality**| High (L1/L2 cache pre-fetching) | Lower (pointer chasing across heap) |

---

## 3. Core Features

1. **TokenBucket Rate Limiter**: High-precision lazy replenishment using `System.nanoTime()` meters TPM (Tokens Per Minute) and RPM without background thread overhead.
2. **CallerRunsPolicy Backpressure**: Forces ingress HTTP threads to execute processing when internal buffers saturate, naturally throttling client edge traffic.
3. **Lock-Free Contention-Free Metrics**: High-frequency metric counters backed by `LongAdder` to prevent CPU cache coherence bus saturation.
4. **Graceful Draining Protocol**: Drains all remaining items upon shutdown, guaranteeing zero data loss during rolling Kubernetes pod deployments.

---

## 4. Verification & Stress Tests

Run the stress test suite:
```bash
javac -d /tmp/test-classes src/main/java/com/enterprise/ingestion/model/*.java src/main/java/com/enterprise/ingestion/core/*.java src/test/java/com/enterprise/ingestion/core/*.java
java -cp /tmp/test-classes com.enterprise.ingestion.core.Day02StressRunner
```

---

## Author
**Pandiyarajan Jeyabalan**  
*Forward Deployed Engineer | Enterprise Platform & AI Systems Architect*  
[LinkedIn](https://www.linkedin.com) • [GitHub](https://github.com)
