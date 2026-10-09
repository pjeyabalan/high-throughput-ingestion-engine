package com.enterprise.ingestion.core;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.LongAdder;

/**
 * Production ThreadFactory for ingestion pipelines.
 *
 * Provides:
 * 1. Predictable thread names for profiling and stack trace analysis (jstack/async-profiler).
 * 2. UncaughtExceptionHandler to prevent silent thread death.
 * 3. Lock-free thread sequence numbering via LongAdder.
 * 4. Zero-dependency logging via Java Platform Logging (System.Logger).
 */
public class NamedThreadFactory implements ThreadFactory {

    private static final Logger log = System.getLogger(NamedThreadFactory.class.getName());

    private final String prefix;
    private final boolean daemon;
    private final LongAdder threadCount = new LongAdder();

    public NamedThreadFactory(String prefix) {
        this(prefix, false);
    }

    public NamedThreadFactory(String prefix, boolean daemon) {
        this.prefix = prefix;
        this.daemon = daemon;
    }

    @Override
    public Thread newThread(Runnable runnable) {
        threadCount.increment();
        String threadName = prefix + "-" + threadCount.sum();
        Thread thread = new Thread(runnable, threadName);
        thread.setDaemon(daemon);
        thread.setUncaughtExceptionHandler((t, ex) ->
                log.log(Level.ERROR, "Uncaught exception in ingestion worker thread: {0}", t.getName(), ex)
        );
        return thread;
    }
}
