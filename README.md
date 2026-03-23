# Distributed High Throughput Rate Limiter

A distributed rate limiter designed for high throughput environments (100M+ calls/min per key).

## How it works

Uses local batching to minimize network calls to the distributed store. Each server instance accumulates request counts locally and periodically flushes them to the shared store, providing approximate accuracy with minimal network overhead.

## Requirements

- Java 21
- Maven

## Build & Test

```bash
mvn clean test
```
