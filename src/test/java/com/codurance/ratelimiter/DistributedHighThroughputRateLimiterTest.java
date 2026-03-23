package com.codurance.ratelimiter;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class DistributedHighThroughputRateLimiterTest {

    @Mock
    private DistributedKeyValueStore store;

    private Clock fixedClock;
    private DistributedHighThroughputRateLimiter rateLimiter;

    @BeforeEach
    void setUp() {
        fixedClock = Clock.fixed(Instant.ofEpochMilli(60_000L), ZoneId.of("UTC"));
        rateLimiter = new DistributedHighThroughputRateLimiter(store, fixedClock);
    }

    @AfterEach
    void tearDown() {
        rateLimiter.shutdown();
    }

    private void stubStoreWithAccumulatingCounter() throws Exception {
        ConcurrentHashMap<String, AtomicInteger> totals = new ConcurrentHashMap<>();
        when(store.incrementByAndExpire(anyString(), anyInt(), anyInt()))
                .thenAnswer(inv -> {
                    String key = inv.getArgument(0, String.class);
                    int delta = inv.getArgument(1, Integer.class);
                    int newTotal = totals.computeIfAbsent(key, k -> new AtomicInteger(0)).addAndGet(delta);
                    return CompletableFuture.completedFuture(newTotal);
                });
    }

    @Test
    void allowsRequestWhenUnderLimit() throws Exception {
        // limit=500, batchSize=25. A single call won't trigger a flush.
        boolean result = rateLimiter.isAllowed("client-1", 500).get();

        assertTrue(result);
        verifyNoInteractions(store);
    }

    @Test
    void allowsMultipleRequestsWithoutFlush() throws Exception {
        // limit=500, batchSize=25. 10 calls won't trigger a flush.
        for (int i = 0; i < 10; i++) {
            assertTrue(rateLimiter.isAllowed("client-1", 500).get());
        }

        verifyNoInteractions(store);
    }

    @Test
    void deniesRequestWhenLocalCountExceedsLimit() throws Exception {
        int limit = 100;
        int allowed = 0;
        stubStoreWithAccumulatingCounter();

        for (int i = 0; i < 200; i++) {
            if (rateLimiter.isAllowed("client-1", limit).get()) {
                allowed++;
            }
        }

        assertTrue(allowed >= limit, "Should allow at least the limit. Allowed: " + allowed);
        assertTrue(allowed < limit * 2, "Should eventually deny. Allowed: " + allowed);
    }

    @Test
    void batchesCallsToStore() throws Exception {
        int limit = 100;
        int batchSize = limit / 20; // 5

        stubStoreWithAccumulatingCounter();

        for (int i = 0; i < batchSize - 1; i++) {
            rateLimiter.isAllowed("client-1", limit).get();
        }

        verify(store, never()).incrementByAndExpire(anyString(), anyInt(), anyInt());

        rateLimiter.isAllowed("client-1", limit).get();

        verify(store, times(1)).incrementByAndExpire(anyString(), eq(batchSize), eq(60));
    }

    @Test
    void flushSendsCorrectDeltaToStore() throws Exception {
        int limit = 200;
        int batchSize = limit / 20; // 10

        stubStoreWithAccumulatingCounter();

        for (int i = 0; i < batchSize; i++) {
            rateLimiter.isAllowed("client-1", limit).get();
        }

        verify(store).incrementByAndExpire(anyString(), eq(batchSize), eq(60));
    }

    @Test
    void failsOpenWhenStoreReturnsFailedFuture() throws Exception {
        when(store.incrementByAndExpire(anyString(), anyInt(), anyInt()))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("store unavailable")));

        // limit=20, batchSize=1 → first call triggers flush
        boolean result = rateLimiter.isAllowed("client-1", 20).get();

        assertTrue(result, "Should allow request when store is unavailable (fail-open)");
    }

    @Test
    void failsOpenWhenStoreThrowsCheckedException() throws Exception {
        when(store.incrementByAndExpire(anyString(), anyInt(), anyInt()))
                .thenThrow(new Exception("connection refused"));

        boolean result = rateLimiter.isAllowed("client-1", 20).get();

        assertTrue(result, "Should allow request when store throws checked exception");
    }

    @Test
    void tracksKeysIndependently() throws Exception {
        stubStoreWithAccumulatingCounter();

        int limit = 5;

        for (int i = 0; i < limit; i++) {
            assertTrue(rateLimiter.isAllowed("client-A", limit).get());
            assertTrue(rateLimiter.isAllowed("client-B", limit).get());
        }

        assertFalse(rateLimiter.isAllowed("client-A", limit).get());
        assertFalse(rateLimiter.isAllowed("client-B", limit).get());
    }

    @Test
    void resetsCounterOnNewWindow() throws Exception {
        stubStoreWithAccumulatingCounter();

        Clock firstWindowClock = Clock.fixed(Instant.ofEpochMilli(60_000L), ZoneId.of("UTC"));
        DistributedHighThroughputRateLimiter limiter =
                new DistributedHighThroughputRateLimiter(store, firstWindowClock);

        int limit = 5;
        for (int i = 0; i < limit; i++) {
            limiter.isAllowed("client-1", limit).get();
        }
        assertFalse(limiter.isAllowed("client-1", limit).get(), "Should deny after reaching limit");
        limiter.shutdown();

        // New window (60 seconds later) uses a different windowKey, so counters are fresh
        Clock secondWindowClock = Clock.fixed(Instant.ofEpochMilli(120_000L), ZoneId.of("UTC"));
        DistributedHighThroughputRateLimiter limiterNewWindow =
                new DistributedHighThroughputRateLimiter(store, secondWindowClock);

        assertTrue(limiterNewWindow.isAllowed("client-1", limit).get(),
                "Should allow requests in a new time window");
        limiterNewWindow.shutdown();
    }

    @Test
    void handlesLimitOfOne() throws Exception {
        stubStoreWithAccumulatingCounter();

        assertTrue(rateLimiter.isAllowed("client-1", 1).get());
        assertFalse(rateLimiter.isAllowed("client-1", 1).get());
    }

    @Test
    void rejectsNullKey() throws Exception {
        assertFalse(rateLimiter.isAllowed(null, 100).get());
    }

    @Test
    void rejectsBlankKey() throws Exception {
        assertFalse(rateLimiter.isAllowed("", 100).get());
        assertFalse(rateLimiter.isAllowed("   ", 100).get());
    }

    @Test
    void rejectsZeroOrNegativeLimit() throws Exception {
        assertFalse(rateLimiter.isAllowed("client-1", 0).get());
        assertFalse(rateLimiter.isAllowed("client-1", -5).get());
    }

    @Test
    void handlesConcurrentAccessSafely() throws Exception {
        stubStoreWithAccumulatingCounter();

        int limit = 500;
        int threadCount = 20;
        int requestsPerThread = 50;

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);
        AtomicInteger allowedCount = new AtomicInteger(0);

        for (int t = 0; t < threadCount; t++) {
            executor.submit(() -> {
                try {
                    startLatch.await();
                    for (int i = 0; i < requestsPerThread; i++) {
                        if (rateLimiter.isAllowed("client-1", limit).get()) {
                            allowedCount.incrementAndGet();
                        }
                    }
                } catch (Exception e) {
                    fail("Unexpected exception: " + e.getMessage());
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        doneLatch.await();
        executor.shutdown();

        // With batching, the allowed count should be approximately the limit.
        // Due to race conditions between local checks and flushes, it may slightly exceed the limit.
        assertTrue(allowedCount.get() >= limit,
                "Should allow at least the limit. Allowed: " + allowedCount.get());
    }

    @Test
    void storeReceivesCorrectExpirationSeconds() throws Exception {
        stubStoreWithAccumulatingCounter();

        // limit=20, batchSize=1 → first call triggers flush
        rateLimiter.isAllowed("client-1", 20).get();

        verify(store).incrementByAndExpire(anyString(), anyInt(), eq(60));
    }

    @Test
    void windowKeyContainsWindowId() throws Exception {
        stubStoreWithAccumulatingCounter();

        // limit=20, batchSize=1 → first call triggers flush
        rateLimiter.isAllowed("xyz", 20).get();

        long expectedWindowId = fixedClock.millis() / 60_000L;
        verify(store).incrementByAndExpire(eq("xyz:" + expectedWindowId), anyInt(), anyInt());
    }

    @Test
    void updatesGlobalCountFromOtherServers() throws Exception {
        // Simulates other servers having already counted requests.
        // After our first flush of 25 requests, the store reports 450 total.
        AtomicInteger storeTotal = new AtomicInteger(425);
        when(store.incrementByAndExpire(anyString(), anyInt(), anyInt()))
                .thenAnswer(inv -> {
                    int delta = inv.getArgument(1, Integer.class);
                    return CompletableFuture.completedFuture(storeTotal.addAndGet(delta));
                });

        int limit = 500;
        int batchSize = limit / 20; // 25

        // First batch: 25 local calls, then flush → store returns 425+25=450
        for (int i = 0; i < batchSize; i++) {
            rateLimiter.isAllowed("client-1", limit).get();
        }

        // After flush, globalCount=450. Remaining budget=50 unflushed calls.
        // Second batch of 25 → estimated goes up to 450+25=475, allowed, triggers flush → store returns 450+25=475
        // Third batch of 25 → estimated goes up to 475+25=500, allowed at 500, then denied
        int allowedAfterFlush = 0;
        for (int i = 0; i < 100; i++) {
            if (rateLimiter.isAllowed("client-1", limit).get()) {
                allowedAfterFlush++;
            }
        }

        // Should allow approximately 50 more (500-450) before being denied
        assertTrue(allowedAfterFlush <= 75,
                "Should restrict based on global count. Allowed after flush: " + allowedAfterFlush);
        assertTrue(allowedAfterFlush >= 25,
                "Should still allow some requests. Allowed after flush: " + allowedAfterFlush);
    }

    @Test
    void handlesHighLimitWithProperBatching() throws Exception {
        stubStoreWithAccumulatingCounter();

        int limit = 10_000;
        int totalCalls = 1_000;

        for (int i = 0; i < totalCalls; i++) {
            rateLimiter.isAllowed("client-1", limit).get();
        }

        // batchSize=500, so ~2 flushes for 1000 calls
        verify(store, atMost(3)).incrementByAndExpire(anyString(), anyInt(), anyInt());
    }
}
