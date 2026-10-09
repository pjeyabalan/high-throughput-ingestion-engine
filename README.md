# High-Throughput Ingestion Engine

[![Java](https://img.shields.io/badge/Java-21-orange.svg)](https://openjdk.org/projects/jdk/21/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Build](https://img.shields.io/badge/Build-Maven-red.svg)](pom.xml)

Production-grade, low-latency ingestion framework engineered for enterprise data integration pipelines. Built to ingest and process high-volume event streams under strict JVM memory boundaries without triggering `OutOfMemoryError: Java heap space`.

---

## 1. The Enterprise Problem

In high-concurrency enterprise pipelines (webhooks, IoT firehoses, legacy database replication), naive asynchronous implementations fail under burst traffic:

```java
// ANTI-PATTERN: Fatal under burst loads
ExecutorService executor = Executors.newFixedThreadPool(16);
```
- **The Failure Vector**: `Executors.newFixedThreadPool` is backed by an **unbounded** `LinkedBlockingQueue` (`Integer.MAX_VALUE`).
- When downstream databases or APIs slow down, incoming tasks accumulate inside the queue.
- At 20k events/second, heap space exhausts within seconds, triggering `java.lang.OutOfMemoryError: Java heap space` or kernel `OOMKilled` (Exit code 137).

---

## 2. Architectural Solution

```
[Concurrent HTTP/Stream Producers]
               |
               v
  offer(item, timeoutMs)  <--- Backpressure Gate
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
  [Downstream Sink / DB Bulk Writer]
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

1. **Strict Backpressure Semantics**: `offer()` with configurable timeouts forces upstream callers to slow down rather than buffer infinitely in memory.
2. **Lock-Free Contention-Free Metrics**: High-frequency metric counters backed by `LongAdder` to prevent CPU cache coherence bus saturation.
3. **Graceful Draining Protocol**: Drains all remaining items upon shutdown, guaranteeing zero data loss during rolling Kubernetes pod deployments.
4. **Predictable Thread Diagnostics**: Custom `NamedThreadFactory` sets thread names and catches unhandled exceptions for fast `jstack` profiling.

---

## 4. Build & Verification

### Prerequisites
- JDK 21+
- Apache Maven 3.9+

### Run Tests
```bash
mvn clean test
```

---

## Author
**Pandiyarajan Jeyabalan**  
*Forward Deployed Engineer | Enterprise Platform & AI Systems Architect*  
[LinkedIn](https://www.linkedin.com) • [GitHub](https://github.com)
