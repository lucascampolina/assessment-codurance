package com.codurance.ratelimiter;

import java.time.Clock;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Distributed rate limiter using local batching for high throughput.
 * Accumulates counts locally and flushes to the shared store periodically,
 * providing approximate accuracy with minimal network overhead.
 */
public class DistributedHighThroughputRateLimiter {

    private static final int WINDOW_SECONDS = 60;
    private static final int BATCH_DIVISOR = 20;
    private static final long CLEANUP_INTERVAL_SECONDS = 120;

    private final DistributedKeyValueStore store;
    private final Clock clock;
    private final ConcurrentHashMap<String, WindowCounter> counters = new ConcurrentHashMap<>();
    private final ScheduledExecutorService cleanupExecutor;

    public DistributedHighThroughputRateLimiter(DistributedKeyValueStore store) {
        this(store, Clock.systemUTC());
    }

    DistributedHighThroughputRateLimiter(DistributedKeyValueStore store, Clock clock) {
        this.store = store;
        this.clock = clock;
        this.cleanupExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "rate-limiter-cleanup");
            t.setDaemon(true);
            return t;
        });
        this.cleanupExecutor.scheduleAtFixedRate(
                this::removeExpiredCounters,
                CLEANUP_INTERVAL_SECONDS,
                CLEANUP_INTERVAL_SECONDS,
                TimeUnit.SECONDS
        );
    }

    /**
     * Returns true if the request is within the allowed limit for this 60-second window.
     */
    public CompletableFuture<Boolean> isAllowed(String key, int limit) {
        if (key == null || key.isBlank()) {
            return CompletableFuture.completedFuture(false);
        }
        if (limit <= 0) {
            return CompletableFuture.completedFuture(false);
        }

        long windowId = currentWindowId();
        String windowKey = key + ":" + windowId;

        WindowCounter counter = counters.computeIfAbsent(windowKey,
                k -> new WindowCounter(windowId));

        if (counter.windowId != windowId) {
            counter = new WindowCounter(windowId);
            counters.put(windowKey, counter);
        }

        long localCount = counter.localCount.incrementAndGet();
        long unflushed = localCount - counter.flushedCount.get();
        long estimated = counter.globalCount.get() + unflushed;

        if (estimated > limit) {
            counter.localCount.decrementAndGet();
            return CompletableFuture.completedFuture(false);
        }

        int batchSize = Math.max(1, limit / BATCH_DIVISOR);
        if (unflushed >= batchSize && counter.flushInProgress.compareAndSet(false, true)) {
            return flush(windowKey, counter, limit);
        }

        return CompletableFuture.completedFuture(true);
    }

    private CompletableFuture<Boolean> flush(String windowKey, WindowCounter counter, int limit) {
        long currentLocal = counter.localCount.get();
        long lastFlushed = counter.flushedCount.get();
        int delta = (int) (currentLocal - lastFlushed);

        if (delta <= 0) {
            counter.flushInProgress.set(false);
            return CompletableFuture.completedFuture(true);
        }

        try {
            return store.incrementByAndExpire(windowKey, delta, WINDOW_SECONDS)
                    .thenApply(globalTotal -> {
                        counter.globalCount.set(globalTotal);
                        counter.flushedCount.addAndGet(delta);
                        counter.flushInProgress.set(false);

                        long currentUnflushed = counter.localCount.get() - counter.flushedCount.get();
                        return (globalTotal + currentUnflushed) <= limit;
                    })
                    .exceptionally(ex -> {
                        // Fail-open: prefer over-allowing over blocking legitimate traffic
                        counter.flushInProgress.set(false);
                        return true;
                    });
        } catch (Exception e) {
            counter.flushInProgress.set(false);
            return CompletableFuture.completedFuture(true);
        }
    }

    private long currentWindowId() {
        return clock.millis() / (WINDOW_SECONDS * 1000L);
    }

    private void removeExpiredCounters() {
        long currentWindow = currentWindowId();
        counters.entrySet().removeIf(entry -> entry.getValue().windowId < currentWindow);
    }

    /** Shuts down the background cleanup executor to release resources. */
    public void shutdown() {
        cleanupExecutor.shutdown();
    }

    static class WindowCounter {
        final long windowId;
        final AtomicLong localCount = new AtomicLong(0);
        final AtomicLong flushedCount = new AtomicLong(0);
        final AtomicLong globalCount = new AtomicLong(0);
        final AtomicBoolean flushInProgress = new AtomicBoolean(false);

        WindowCounter(long windowId) {
            this.windowId = windowId;
        }
    }
}
