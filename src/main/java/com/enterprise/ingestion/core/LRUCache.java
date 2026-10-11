package com.enterprise.ingestion.core;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Thread-Safe O(1) Least Recently Used (LRU) Cache from Scratch.
 *
 * Built without java.util.LinkedHashMap for Big Tech & Fintech loops (Capital One, PayPal, Walmart, Grab):
 * 1. O(1) get() and put() using a HashMap + custom Doubly-Linked List.
 * 2. Dummy Head and Tail sentinels eliminate edge-case null-pointer checks during node splicing.
 * 3. Thread-safe concurrency via ReentrantReadWriteLock (optimizes concurrent read hits).
 * 4. Metrics instrumentation using LongAdder tracking hits, misses, and evictions for Prometheus.
 *
 * In enterprise AI gateways, this functions as the high-speed In-Memory Semantic Prompt Cache.
 *
 * @param <K> Key type (e.g. Prompt SHA-256 hash or Transaction ID)
 * @param <V> Value type (e.g. Cached LLM response or Account Profile)
 */
public class LRUCache<K, V> {

    private final int capacity;
    private final Map<K, Node<K, V>> map;
    private final Node<K, V> head;
    private final Node<K, V> tail;

    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock.WriteLock writeLock = rwLock.writeLock();

    private final LongAdder hitCount = new LongAdder();
    private final LongAdder missCount = new LongAdder();
    private final LongAdder evictionCount = new LongAdder();

    private static class Node<K, V> {
        K key;
        V value;
        Node<K, V> prev;
        Node<K, V> next;

        Node(K key, V value) {
            this.key = key;
            this.value = value;
        }
    }

    public LRUCache(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("Cache capacity must be strictly positive");
        }
        this.capacity = capacity;
        this.map = new HashMap<>(capacity);

        // Dummy head and tail sentinels
        this.head = new Node<>(null, null);
        this.tail = new Node<>(null, null);
        head.next = tail;
        tail.prev = head;
    }

    /**
     * O(1) retrieval. Moves accessed item to the head (most recently used).
     */
    public Optional<V> get(K key) {
        Objects.requireNonNull(key, "Key cannot be null");

        writeLock.lock(); // Write lock required because get() modifies the doubly-linked list order
        try {
            Node<K, V> node = map.get(key);
            if (node == null) {
                missCount.increment();
                return Optional.empty();
            }

            // Move accessed node to head (MRU)
            moveToHead(node);
            hitCount.increment();
            return Optional.of(node.value);
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * O(1) insertion / update. Evicts least recently used item from tail if capacity is exceeded.
     */
    public void put(K key, V value) {
        Objects.requireNonNull(key, "Key cannot be null");
        Objects.requireNonNull(value, "Value cannot be null");

        writeLock.lock();
        try {
            Node<K, V> existingNode = map.get(key);

            if (existingNode != null) {
                existingNode.value = value;
                moveToHead(existingNode);
            } else {
                Node<K, V> newNode = new Node<>(key, value);
                map.put(key, newNode);
                addFirst(newNode);

                if (map.size() > capacity) {
                    Node<K, V> lru = removeTail();
                    if (lru != null) {
                        map.remove(lru.key);
                        evictionCount.increment();
                    }
                }
            }
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Checks if key exists without altering recency ordering.
     */
    public boolean containsKey(K key) {
        rwLock.readLock().lock();
        try {
            return map.containsKey(key);
        } finally {
            rwLock.readLock().unlock();
        }
    }

    /**
     * Current number of cached elements.
     */
    public int size() {
        rwLock.readLock().lock();
        try {
            return map.size();
        } finally {
            rwLock.readLock().unlock();
        }
    }

    public int getCapacity() { return capacity; }
    public long getHitCount() { return hitCount.sum(); }
    public long getMissCount() { return missCount.sum(); }
    public long getEvictionCount() { return evictionCount.sum(); }

    public double getHitRate() {
        long hits = hitCount.sum();
        long total = hits + missCount.sum();
        return total > 0 ? (double) hits / total : 0.0;
    }

    // --- Doubly-Linked List Internal Helper Methods (Must be called inside writeLock) ---

    private void addFirst(Node<K, V> node) {
        node.next = head.next;
        node.prev = head;
        head.next.prev = node;
        head.next = node;
    }

    private void removeNode(Node<K, V> node) {
        node.prev.next = node.next;
        node.next.prev = node.prev;
    }

    private void moveToHead(Node<K, V> node) {
        removeNode(node);
        addFirst(node);
    }

    private Node<K, V> removeTail() {
        if (tail.prev == head) {
            return null; // Cache is empty
        }
        Node<K, V> lru = tail.prev;
        removeNode(lru);
        return lru;
    }
}
