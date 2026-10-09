package com.enterprise.ingestion.core;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Production-Grade Token Bucket Rate Limiter.
 *
 * Engineered for Enterprise AI Gateways & Distributed Ingestion:
 * 1. Lazy Refill: Computes token replenishment mathematically on-demand via System.nanoTime()
 *    (Zero background thread overhead, zero timer context switching).
 * 2. Precision Token Control: Meters both Request-Per-Second (RPM) and Token-Per-Minute (TPM)
 *    constraints for external LLM APIs (Azure OpenAI, Anthropic, Bedrock).
 * 3. Bounded Blocking: acquire() blocks callers with precise timeouts, providing natural
 *    backpressure instead of immediately throwing 429 errors.
 */
public class TokenBucketRateLimiter {

    private static final Logger log = System.getLogger(TokenBucketRateLimiter.class.getName());

    private final double maxTokens;
    private final double refillRatePerSecond;

    private double availableTokens;
    private long lastRefillNanos;

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition tokensAvailable = lock.newCondition();

    public TokenBucketRateLimiter(double maxTokens, double refillRatePerSecond) {
        if (maxTokens <= 0) {
            throw new IllegalArgumentException("maxTokens must be strictly positive");
        }
        if (refillRatePerSecond <= 0) {
            throw new IllegalArgumentException("refillRatePerSecond must be strictly positive");
        }
        this.maxTokens = maxTokens;
        this.refillRatePerSecond = refillRatePerSecond;
        this.availableTokens = maxTokens;
        this.lastRefillNanos = System.nanoTime();

        log.log(Level.INFO, "Initialized TokenBucketRateLimiter [maxTokens={0}, refillRatePerSec={1}]", 
                maxTokens, refillRatePerSecond);
    }

    /**
     * Non-blocking attempt to acquire requested tokens.
     *
     * @param tokens Number of tokens to consume
     * @return true if acquired immediately, false if insufficient tokens
     */
    public boolean tryAcquire(int tokens) {
        if (tokens <= 0) {
            throw new IllegalArgumentException("Tokens requested must be positive");
        }

        lock.lock();
        try {
            refill();
            if (availableTokens >= tokens) {
                availableTokens -= tokens;
                return true;
            }
            return false;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Blocking acquire: Waits up to the specified timeout for tokens to replenish.
     *
     * @param tokens Number of tokens required
     * @param timeout Maximum wait duration
     * @param unit Time unit for timeout
     * @return true if tokens acquired within timeout, false otherwise
     * @throws InterruptedException If the waiting thread is interrupted
     */
    public boolean acquire(int tokens, long timeout, TimeUnit unit) throws InterruptedException {
        if (tokens <= 0) {
            throw new IllegalArgumentException("Tokens requested must be positive");
        }
        if (tokens > maxTokens) {
            throw new IllegalArgumentException("Requested tokens (" + tokens + ") exceed max bucket capacity (" + maxTokens + ")");
        }

        long timeoutNanos = unit.toNanos(timeout);
        long deadlineNanos = System.nanoTime() + timeoutNanos;

        lock.lock();
        try {
            while (true) {
                refill();
                if (availableTokens >= tokens) {
                    availableTokens -= tokens;
                    return true;
                }

                long remainingNanos = deadlineNanos - System.nanoTime();
                if (remainingNanos <= 0) {
                    return false; // Timed out waiting for tokens
                }

                // Calculate exact nanoseconds needed until enough tokens replenish
                double tokensNeeded = tokens - availableTokens;
                long nanosToWait = (long) Math.ceil((tokensNeeded / refillRatePerSecond) * 1_000_000_000.0);
                long waitNanos = Math.min(nanosToWait, remainingNanos);

                if (waitNanos > 0) {
                    tokensAvailable.await(waitNanos, TimeUnit.NANOSECONDS);
                }
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Lazily replenishes tokens based on elapsed nanoseconds since last check.
     */
    private void refill() {
        long now = System.nanoTime();
        long elapsedNanos = now - lastRefillNanos;

        if (elapsedNanos > 0) {
            double tokensToAdd = (elapsedNanos / 1_000_000_000.0) * refillRatePerSecond;
            this.availableTokens = Math.min(maxTokens, availableTokens + tokensToAdd);
            this.lastRefillNanos = now;
            tokensAvailable.signalAll();
        }
    }

    public double getAvailableTokens() {
        lock.lock();
        try {
            refill();
            return availableTokens;
        } finally {
            lock.unlock();
        }
    }

    public double getMaxTokens() { return maxTokens; }
    public double getRefillRatePerSecond() { return refillRatePerSecond; }
}
