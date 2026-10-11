package com.enterprise.ingestion.core;

import java.io.Serial;
import java.io.Serializable;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Thread-Safe High-Performance Singleton with Multi-Vector Attack Defenses.
 *
 * Implements the verified industry pattern asked in Big Tech & Fintech loops (Goldman Sachs, Walmart, Morgan Stanley, Capital One):
 * 1. Double-Checked Locking (DCL) with volatile to prevent CPU instruction reordering.
 * 2. Local-variable caching (localRef) to minimize volatile memory bus reads on happy path.
 * 3. Reflection Guard: Constructor checks initialization state and throws IllegalStateException.
 * 4. Serialization Guard: readResolve() hook guarantees canonical instance preservation.
 * 5. Cloning Guard: clone() throws CloneNotSupportedException.
 * 6. Includes modern Joshua Bloch Enum Singleton implementation for comparison.
 */
public class IngestionEngineRegistry implements Serializable, Cloneable {

    @Serial
    private static final long serialVersionUID = 1L;

    private static final System.Logger logger = System.getLogger(IngestionEngineRegistry.class.getName());

    // Volatile is mandatory: establishes a happens-before relationship and prevents
    // the CPU/JIT from reordering memory allocation before object constructor execution.
    private static volatile IngestionEngineRegistry instance;

    // Secondary latch to defend against reflection instantiation before instance assignment
    private static volatile boolean isInstantiated = false;

    private final String engineId;
    private final long initializedTimestampMs;
    private final Map<String, String> configurationProperties = new ConcurrentHashMap<>();

    /**
     * Private constructor fortified against Reflection attacks.
     */
    private IngestionEngineRegistry() {
        synchronized (IngestionEngineRegistry.class) {
            if (instance != null || isInstantiated) {
                throw new IllegalStateException(
                    "Singleton violation: Instance already created. Reflective constructor invocation is prohibited."
                );
            }
            isInstantiated = true;
        }

        this.engineId = "INGESTION-NODE-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();
        this.initializedTimestampMs = System.currentTimeMillis();
        this.configurationProperties.put("cluster.mode", "DISTRIBUTED_ACTIVE");
        this.configurationProperties.put("memory.guardrail", "STRICT_BOUNDED");

        logger.log(System.Logger.Level.INFO, "Initialized IngestionEngineRegistry: " + engineId);
    }

    /**
     * Double-Checked Locking (DCL) pattern with localRef optimization.
     *
     * Why localRef?
     * Reading a volatile variable requires checking CPU cache coherence (memory barrier).
     * By copying 'instance' into a local register variable, we only read volatile memory once
     * when instance is already initialized, speeding up hot path execution.
     */
    public static IngestionEngineRegistry getInstance() {
        IngestionEngineRegistry localRef = instance;
        if (localRef == null) { // Check 1: No locking for already-initialized instances
            synchronized (IngestionEngineRegistry.class) {
                localRef = instance;
                if (localRef == null) { // Check 2: Locked check to prevent race condition
                    instance = localRef = new IngestionEngineRegistry();
                }
            }
        }
        return localRef;
    }

    /**
     * Serialization Defense:
     * When deserialized, the JVM calls readResolve() if present.
     * Returning getInstance() discards the synthetic deserialized object and preserves singleton uniqueness.
     */
    @Serial
    private Object readResolve() {
        return getInstance();
    }

    /**
     * Cloning Defense:
     * Prevents duplicate instances via Object.clone().
     */
    @Override
    protected Object clone() throws CloneNotSupportedException {
        throw new CloneNotSupportedException("Singleton cloning is strictly prohibited for IngestionEngineRegistry.");
    }

    public String getEngineId() {
        return engineId;
    }

    public long getInitializedTimestampMs() {
        return initializedTimestampMs;
    }

    public void setConfig(String key, String value) {
        Objects.requireNonNull(key, "Config key cannot be null");
        Objects.requireNonNull(value, "Config value cannot be null");
        configurationProperties.put(key, value);
    }

    public String getConfig(String key) {
        return configurationProperties.get(key);
    }

    public Map<String, String> getAllConfigs() {
        return Collections.unmodifiableMap(configurationProperties);
    }

    /**
     * Resets the singleton state strictly for isolated test suites.
     * Package-private.
     */
    static synchronized void resetForTesting() {
        instance = null;
        isInstantiated = false;
    }

    // =========================================================================
    // Modern Enum-Based Singleton Alternative (Joshua Bloch, Effective Java)
    // =========================================================================

    /**
     * The Enum Singleton:
     * - Guaranteed 100% thread-safe by JVM classloader specification.
     * - Immune to reflection attacks (JVM explicitly prevents Constructor.newInstance on enums).
     * - Immune to serialization duplication without requiring readResolve().
     */
    public enum EnumIngestionRegistry {
        INSTANCE;

        private final String clusterId = "CLUSTER-ENUM-" + UUID.randomUUID().toString().substring(0, 8);
        private final Map<String, String> metadata = new ConcurrentHashMap<>();

        EnumIngestionRegistry() {
            metadata.put("protocol", "GOSSIP_V2");
        }

        public String getClusterId() {
            return clusterId;
        }

        public void putMetadata(String k, String v) {
            metadata.put(k, v);
        }

        public String getMetadata(String k) {
            return metadata.get(k);
        }
    }
}
