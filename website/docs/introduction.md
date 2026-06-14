# Introduction

`@nest-native/kafka` is a community NestJS integration for Apache Kafka. It is a
decorator-first transport built on Confluent's officially supported
[`@confluentinc/kafka-javascript`](https://github.com/confluentinc/confluent-kafka-javascript)
client. The goal is for Kafka consumers and producers to feel like a first-class
Nest transport — preserving the `@MessagePattern` / `@EventPattern` ergonomics of
`@nestjs/microservices` — while solving the correctness gaps the kafkajs-based
official transport accumulated.

The library does not replace the Confluent client, your schemas, your serializers,
or your broker operations. Your application still owns those choices. This package
supplies the Nest-facing integration layer:

- `KafkaModule.forRoot()` / `forRootAsync()` for transport registration and
  `forFeature([HandlerClass])` for grouping consumers.
- `@KafkaConsumer` (class) and `@KafkaHandler` (method) decorators with the full
  Nest enhancer pipeline.
- `@KafkaMessage()`, `@KafkaHeaders()`, `@KafkaCtx()`, and `@KafkaBatch()`
  parameter decorators.
- `KafkaProducerService` (`send`, `sendBatch`, `transactional`) and
  `@InjectKafkaProducer()` for direct producer access.
- Error mapping from Nest exceptions to commit/retry behavior.
- Batch consumption, per-topic concurrency, backpressure, and graceful shutdown.
- `KafkaTestModule` and producer mocks for tests without a broker.

## Design Goals

The package should feel native in NestJS projects and faithful to Kafka:

- Nest owns dependency injection, module boundaries, the enhancer pipeline, and
  lifecycle cleanup.
- The Confluent client owns the wire protocol, partitioning, and consumer-group
  coordination. The transport is a `CustomTransportStrategy`, not a new transport
  contract.
- Kafka semantics are never hidden. Offsets commit only after a handler returns
  successfully; rebalances are handled explicitly; exceptions surface.
- Optional integrations stay optional. The Confluent client, `class-validator`,
  and Zod are peer capabilities, not runtime dependencies pulled into every app.

## When To Use It

Use this package when your Nest application consumes from or produces to Kafka and
you want:

- Handlers as decorated provider methods that run through guards, interceptors,
  pipes, and filters.
- A producer service you can inject, with single, batch, and transactional sends.
- Correct behavior under rebalances, slow consumers, and poison messages.
- A documented migration path off `@nestjs/microservices`'s Kafka transport.

For the rationale and the issues it addresses, see [Why Native](why-native.md).
For the first runnable setup, continue with [Quick Start](quick-start.md).
