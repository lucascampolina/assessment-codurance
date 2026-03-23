# Distributed High Throughput Rate Limiter

Rate limiter distribuído para ambientes de alta throughput (100M+ chamadas/min por key).

## Como funciona

Usa batching local para minimizar chamadas de rede ao store distribuído. Cada instância acumula contagens localmente e faz flush periodicamente, garantindo precisão aproximada com overhead mínimo de rede.

## Requisitos

- Java 21
- Maven

## Build e Testes

```bash
mvn clean test
```

## Estrutura

```
src/main/java/com/codurance/ratelimiter/
  DistributedKeyValueStore.java                  - Interface do store distribuído
  DistributedHighThroughputRateLimiter.java      - Implementação do rate limiter
src/test/java/com/codurance/ratelimiter/
  DistributedHighThroughputRateLimiterTest.java  - Testes unitários
```
