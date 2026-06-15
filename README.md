# @nest-native/kafka

<p align="center">Decorator-first NestJS Kafka integration built on Confluent's officially supported @confluentinc/kafka-javascript client.</p>

<p align="center">
  <a href="https://www.npmjs.com/package/@nest-native/kafka"><img src="https://img.shields.io/npm/v/@nest-native/kafka.svg" alt="NPM Version" /></a>
  <a href="https://www.npmjs.com/package/@nest-native/kafka"><img src="https://img.shields.io/npm/dm/@nest-native/kafka.svg" alt="NPM Downloads" /></a>
  <a href="https://opensource.org/licenses/MIT"><img src="https://img.shields.io/badge/license-MIT-green.svg" alt="Package License" /></a>
  <img src="https://img.shields.io/badge/coverage-100%25-brightgreen.svg" alt="Test Coverage" />
  <a href="https://nest-native.dev/kafka/"><img src="https://img.shields.io/badge/docs-%40nest--native%2Fkafka-0f766e.svg" alt="Documentation" /></a>
</p>

> [!NOTE]
> **Status: v0.1 — stable.** The full v1 surface is shipped: the module
> (`KafkaModule.forRoot()` / `forRootAsync()` / `forFeature()`), the
> `KafkaProducerService` (`send`, `sendBatch`, `transactional`),
> `@InjectKafkaProducer()`, the consumer decorators (`@KafkaConsumer`,
> `@KafkaHandler`) with the full Nest enhancer pipeline, the parameter decorators
> (`@KafkaMessage`, `@KafkaHeaders`, `@KafkaCtx`, `@KafkaBatch`), error mapping,
> graceful shutdown, batch consumption, per-topic concurrency, backpressure, the
> testing utilities (`KafkaTestModule`, `InMemoryKafkaBroker`,
> `createMockKafkaProducer`), the sample catalog, and the
> [documentation site](https://nest-native.dev/kafka/). The published package keeps
> `"dependencies": {}`.

## What This Is

`@nest-native/kafka` is a community NestJS integration for Kafka consumers and
producers built on Confluent's officially supported
[`@confluentinc/kafka-javascript`](https://github.com/confluentinc/confluent-kafka-javascript)
client. The goal is a decorator-first, Nest-native transport that preserves the
`@MessagePattern` / `@EventPattern` ergonomics of `@nestjs/microservices` so
teams can migrate — while keeping the **full Nest enhancer pipeline** (guards,
pipes, interceptors, filters) intact on handler methods.

It is a transport-only integration. It wraps the Confluent client; it does not
re-implement or hide it.

## Why

The official `@nestjs/microservices` Kafka transport is built on `kafkajs`,
which the community widely treats as effectively unmaintained (see
[`nestjs/nest#13223`](https://github.com/nestjs/nest/issues/13223), where
Confluent staff offered their client as the replacement). The official transport
also carries unresolved correctness issues:

- **Sequential per-topic processing** ([`#12703`](https://github.com/nestjs/nest/issues/12703))
- **Rebalance hangs** ([`#12355`](https://github.com/nestjs/nest/issues/12355))
- **Exception swallowing** ([`#9679`](https://github.com/nestjs/nest/issues/9679))

This package's headline differentiators:

- **Confluent client, not kafkajs:** built on the officially supported,
  actively maintained `@confluentinc/kafka-javascript` (v1.9+).
- **Rebalance-safe consumption:** offsets commit only after a successful handler
  return; in-flight messages complete or are explicitly aborted.
- **Per-topic concurrency:** addresses `#12703` with a documented default and an
  opt-out, instead of forced sequential processing.
- **A documented migration path** from the `@nestjs/microservices` Kafka
  transport.

## Compatibility

| Runtime | Supported line |
| --- | --- |
| Node.js | `>=20` |
| NestJS | `11.x` |
| `@confluentinc/kafka-javascript` | `^1.9` (pin major; tracks librdkafka) |
| Validation | class-validator and Zod, both app-owned |

The published package keeps `"dependencies": {}`. The Confluent client and the
NestJS packages are declared as `peerDependencies`, so applications install only
the ecosystems they actually use.

## Repository Layout

This repository contains:

- [`packages/kafka`](packages/kafka): the `@nest-native/kafka` integration package
- [`sample`](sample): the runnable sample catalog (producer basics, consumer
  enhancers, headers/context/errors, batch + concurrency, transactions, and the
  `@nestjs/microservices` migration)
- [`website`](website): the [documentation site](https://nest-native.dev/kafka/) source
- [`scripts`](scripts): quality, coverage, complexity, and release-check helpers
- [`CONTRIBUTING.md`](CONTRIBUTING.md): contributor workflow, including the
  sample/library PR separation rule
- [`CHANGELOG.md`](CHANGELOG.md): release history and unreleased changes
- [`SECURITY.md`](SECURITY.md): vulnerability reporting and project security boundaries
- [`GUIDELINES_NEST_KAFKA.md`](GUIDELINES_NEST_KAFKA.md): the project constitution

## Installation

```bash
npm i @nest-native/kafka @confluentinc/kafka-javascript
```

Required peers:

```bash
npm i @nestjs/common @nestjs/core @nestjs/microservices reflect-metadata rxjs
```

## Usage

Wire the module with your broker connection:

```ts
import { Module } from '@nestjs/common';
import { KafkaModule } from '@nest-native/kafka';

@Module({
  imports: [
    KafkaModule.forRoot({
      clientId: 'orders-service',
      client: { brokers: ['localhost:9092'] },
    }),
  ],
})
export class AppModule {}
```

Async configuration is supported through `KafkaModule.forRootAsync()`:

```ts
KafkaModule.forRootAsync({
  inject: [ConfigService],
  useFactory: (config: ConfigService) => ({
    clientId: config.getOrThrow('KAFKA_CLIENT_ID'),
    client: { brokers: config.getOrThrow('KAFKA_BROKERS').split(',') },
  }),
});
```

Publish messages with the injected `KafkaProducerService`, and consume them with
`@KafkaConsumer` / `@KafkaHandler` classes that run through the full Nest enhancer
pipeline (guards, interceptors, pipes, filters):

```ts
import { Injectable } from '@nestjs/common';
import {
  KafkaConsumer,
  KafkaHandler,
  KafkaMessage,
  KafkaProducerService,
} from '@nest-native/kafka';

@Injectable()
export class OrdersService {
  constructor(private readonly producer: KafkaProducerService) {}

  placeOrder(id: string) {
    return this.producer.send({
      topic: 'orders.placed',
      messages: [{ key: id, value: JSON.stringify({ id }) }],
    });
  }
}

@Injectable()
@KafkaConsumer('orders.placed', { groupId: 'orders-service' })
export class OrdersConsumer {
  @KafkaHandler()
  handle(@KafkaMessage() order: { id: string }) {
    // runs after guards, interceptors, and pipes; exception filters wrap it
  }
}
```

Feature modules register their consumer/handler classes through
`KafkaModule.forFeature()`:

```ts
@Module({
  imports: [KafkaModule.forFeature([OrdersConsumer])],
})
export class OrdersModule {}
```

`forRoot` and `forRootAsync` return a global `DynamicModule` by default — pass
`isGlobal: false` to scope them to a single module boundary. `forFeature`
returns a non-global module that registers and exports the supplied handlers.

The full API — transactions, batch consumption, per-topic concurrency, error
mapping, graceful shutdown, the parameter decorators, and the testing utilities —
is covered in the package [README](packages/kafka/README.md) and the
[documentation site](https://nest-native.dev/kafka/).

## Quality Gates

The repository ships the same review posture as its sibling `@nest-native`
packages, using `node:test` and `c8`:

- package build, typecheck, and coverage on Node.js 20 and 22
- coverage with `c8`, enforced at 100% for statements, branches, functions, and lines
- sticky PR comments for coverage, test performance, and cognitive complexity
- cognitive complexity enforcement with SonarJS threshold `15`
- package tarball validation and README link validation
- supply-chain audit for high-severity issues
- a real-broker integration job that runs a produce → consume round-trip, a
  transactional commit, and per-topic concurrency against a single-node KRaft
  Kafka (skipped locally unless `KAFKA_BROKERS` is set)

Run the local gate with:

```bash
npm run ci
```

## What Shipped in v0.1

The whole v1 surface is in place and published:

1. **The module** — `KafkaModule.forRoot()` / `forRootAsync()` / `forFeature()`,
   the Kafka driver, and the shared producer.
2. **Producer service** — `KafkaProducerService` (`send`, `sendBatch`,
   `transactional`) and `@InjectKafkaProducer()` for the raw Confluent producer.
3. **Consumers** — `@KafkaConsumer` + `@KafkaHandler` with the full Nest enhancer
   pipeline (guards, interceptors, pipes, filters) and request-scoped DI.
4. **Parameter decorators, error mapping, graceful shutdown** — `@KafkaMessage`,
   `@KafkaHeaders`, `@KafkaCtx`, `@KafkaBatch`; commit-or-retry error mapping
   (`#9679`); in-flight draining on shutdown.
5. **Batch consume + per-topic concurrency** — addresses sequential per-topic
   processing (`#12703`) and rebalance-safe offsets (`#12355`), plus backpressure.
6. **Transactional producer helper** — `transactional(work)` with `sendOffsets`
   for the consume-process-produce pattern.
7. **Testing utilities** — `KafkaTestModule`, `InMemoryKafkaBroker`,
   `createMockKafkaProducer`, and a migration guide from `@nestjs/microservices`.
8. **Documentation site and the sample catalog**, plus a real-broker CI
   integration test running against a single-node KRaft Kafka.

See [CHANGELOG.md](CHANGELOG.md) for the per-release detail.

## License

[MIT](LICENSE) © 2026 Rodrigo Nogueira.

Part of the [nest-native](https://github.com/nest-native) family, alongside
[@nest-native/drizzle](https://github.com/nest-native/drizzle) and
[@nest-native/trpc](https://github.com/nest-native/trpc).
