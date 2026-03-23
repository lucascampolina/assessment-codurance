package com.codurance.ratelimiter;

import java.util.concurrent.CompletableFuture;

/**
 * Existing data access layer for a distributed key-value store shared across all server instances.
 * Increments a key by a delta and sets an expiration. The expiration is only applied on key creation.
 */
public interface DistributedKeyValueStore {

    CompletableFuture<Integer> incrementByAndExpire(String key, int delta, int expirationSeconds) throws Exception;
}
