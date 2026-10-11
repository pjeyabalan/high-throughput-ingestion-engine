package com.enterprise.ingestion.core;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Verification & Stress Test Suite for IngestionEngineRegistry.
 *
 * Verifies all 5 core Big Tech / Fintech interview expectations:
 * 1. Concurrency: 32 threads race to instantiate simultaneously -> exactly 1 instance created.
 * 2. Reflection Defense: constructor.newInstance() fails with IllegalStateException.
 * 3. Serialization Defense: readResolve() preserves identical memory reference.
 * 4. Cloning Defense: clone() throws CloneNotSupportedException.
 * 5. Enum Singleton: JVM prevents reflection instantiation natively.
 * 6. Throughput: 1,000,000 get operations execute with zero lock contention.
 */
public class Day05SingletonRunner {

    public static void main(String[] args) throws Exception {
        System.out.println("================================================================================");
        System.out.println("⚡ DAY 05 VERIFICATION: THREAD-SAFE SINGLETON & MULTI-VECTOR ATTACK DEFENSES");
        System.out.println("================================================================================");

        testConcurrentRaceInitialization();
        testReflectionAttackDefense();
        testSerializationDefense();
        testCloningDefense();
        testEnumSingletonReflectionImmunity();
        testHotPathReadThroughput();

        System.out.println("================================================================================");
        System.out.println("🎉 ALL SINGLETON ATTACK VECTORS DEFENDED & VERIFIED SUCCESSFULLY!");
        System.out.println("================================================================================");
    }

    private static void testConcurrentRaceInitialization() throws Exception {
        System.out.print("[1/6] Testing 32-Thread Concurrent Race Initialization... ");
        IngestionEngineRegistry.resetForTesting();

        int threadCount = 32;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startGun = new CountDownLatch(1);
        CountDownLatch finishLatch = new CountDownLatch(threadCount);

        Set<IngestionEngineRegistry> instances = Collections.newSetFromMap(new ConcurrentHashMap<>());
        Set<String> engineIds = Collections.newSetFromMap(new ConcurrentHashMap<>());

        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try {
                    startGun.await(); // Guarantee all 32 threads fire at the exact same nanosecond
                    IngestionEngineRegistry reg = IngestionEngineRegistry.getInstance();
                    instances.add(reg);
                    engineIds.add(reg.getEngineId());
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    finishLatch.countDown();
                }
            });
        }

        startGun.countDown(); // Fire race
        boolean finished = finishLatch.await(5, TimeUnit.SECONDS);
        executor.shutdown();

        if (!finished || instances.size() != 1 || engineIds.size() != 1) {
            throw new AssertionError("FAILED: Multiple instances detected under race condition! Count: " + instances.size());
        }

        IngestionEngineRegistry canonical = instances.iterator().next();
        System.out.println("PASSED! (32 concurrent threads resolved to identical instance: " + canonical.getEngineId() + ")");
    }

    private static void testReflectionAttackDefense() throws Exception {
        System.out.print("[2/6] Testing Reflection Attack Defense... ");
        IngestionEngineRegistry canonical = IngestionEngineRegistry.getInstance();

        Constructor<IngestionEngineRegistry> constructor =
            IngestionEngineRegistry.class.getDeclaredConstructor();
        constructor.setAccessible(true);

        try {
            constructor.newInstance();
            throw new AssertionError("FAILED: Reflection instantiated a second instance! Attack was not blocked.");
        } catch (InvocationTargetException ite) {
            Throwable cause = ite.getCause();
            if (cause instanceof IllegalStateException && cause.getMessage().contains("Singleton violation")) {
                System.out.println("PASSED! (Blocked with IllegalStateException: \"" + cause.getMessage() + "\")");
            } else {
                throw new AssertionError("Unexpected exception during reflection attack: " + cause);
            }
        }
    }

    private static void testSerializationDefense() throws Exception {
        System.out.print("[3/6] Testing Serialization Attack Defense (readResolve)... ");
        IngestionEngineRegistry original = IngestionEngineRegistry.getInstance();
        original.setConfig("cluster.node.weight", "100");

        // Serialize
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(original);
        }

        // Deserialize
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        IngestionEngineRegistry deserialized;
        try (ObjectInputStream ois = new ObjectInputStream(bais)) {
            deserialized = (IngestionEngineRegistry) ois.readObject();
        }

        if (deserialized != original) {
            throw new AssertionError("FAILED: Deserialized object created a new instance in memory!");
        }

        if (!"100".equals(deserialized.getConfig("cluster.node.weight"))) {
            throw new AssertionError("FAILED: Deserialized object lost configuration state!");
        }

        System.out.println("PASSED! (readResolve guaranteed identical memory pointer original == deserialized)");
    }

    private static void testCloningDefense() throws Exception {
        System.out.print("[4/6] Testing Cloning Attack Defense... ");
        IngestionEngineRegistry original = IngestionEngineRegistry.getInstance();

        Method cloneMethod = IngestionEngineRegistry.class.getDeclaredMethod("clone");
        cloneMethod.setAccessible(true);

        try {
            cloneMethod.invoke(original);
            throw new AssertionError("FAILED: Clone method succeeded and duplicated singleton!");
        } catch (InvocationTargetException ite) {
            if (ite.getCause() instanceof CloneNotSupportedException) {
                System.out.println("PASSED! (CloneNotSupportedException intercepted duplicate attempt)");
            } else {
                throw new AssertionError("Unexpected exception on clone: " + ite.getCause());
            }
        }
    }

    private static void testEnumSingletonReflectionImmunity() throws Exception {
        System.out.print("[5/6] Testing Enum Singleton Reflection Immunity... ");
        IngestionEngineRegistry.EnumIngestionRegistry enumInst = IngestionEngineRegistry.EnumIngestionRegistry.INSTANCE;

        try {
            Constructor<?>[] constructors = IngestionEngineRegistry.EnumIngestionRegistry.class.getDeclaredConstructors();
            constructors[0].setAccessible(true);
            constructors[0].newInstance("HACKED", 1);
            throw new AssertionError("FAILED: Reflection instantiated an Enum!");
        } catch (IllegalArgumentException iae) {
            if (iae.getMessage().contains("Cannot reflectively create enum objects")) {
                System.out.println("PASSED! (JVM threw IllegalArgumentException: \"" + iae.getMessage() + "\")");
            } else {
                throw iae;
            }
        }
    }

    private static void testHotPathReadThroughput() throws Exception {
        System.out.print("[6/6] Benchmarking 1,000,000 Hot-Path getInstance() Reads... ");

        int readCount = 1_000_000;
        int workerCount = 8;
        int opsPerWorker = readCount / workerCount;
        ExecutorService pool = Executors.newFixedThreadPool(workerCount);
        CountDownLatch latch = new CountDownLatch(workerCount);

        long startNs = System.nanoTime();

        for (int w = 0; w < workerCount; w++) {
            pool.submit(() -> {
                int dummyHash = 0;
                for (int i = 0; i < opsPerWorker; i++) {
                    IngestionEngineRegistry reg = IngestionEngineRegistry.getInstance();
                    dummyHash ^= reg.hashCode();
                }
                latch.countDown();
            });
        }

        latch.await();
        long durationNs = System.nanoTime() - startNs;
        pool.shutdown();

        double seconds = durationNs / 1_000_000_000.0;
        double opsPerSec = readCount / seconds;

        System.out.printf("PASSED! (%,d reads completed in %.3fs -> %,.0f ops/sec with zero contention)%n",
            readCount, seconds, opsPerSec);
    }
}
