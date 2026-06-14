# @nest-native/kafka

<p align="center">Decorator-first NestJS Kafka integration built on Confluent's officially supported @confluentinc/kafka-javascript client.</p>

<p align="center">
  <a href="https://www.npmjs.com/package/@nest-native/kafka"><img src="https://img.shields.io/npm/v/@nest-native/kafka.svg" alt="NPM Version" /></a>
  <a href="https://opensource.org/licenses/MIT"><img src="https://img.shields.io/badge/license-MIT-green.svg" alt="Package License" /></a>
  <img src="https://img.shields.io/badge/coverage-100%25-brightgreen.svg" alt="Test Coverage" />
  <img src="https://img.shields.io/badge/status-scaffold-orange.svg" alt="Status: scaffold" />
</p>

> [!WARNING]
> **Status: scaffold / under construction.** This repository is at its bootstrap
> milestone (`v0.0.1-scaffold`). The npm workspace builds, typechecks, tests at
> 100% coverage, and is CI-green, but the public transport API is not
> implemented yet. Only `KafkaModule.forRoot()` / `KafkaModule.forRootAsync()` /
> `KafkaModule.forFeature()` exist. The consumer decorators, parameter
> decorators, the producer service, and the sample catalog arrive in later
> milestones. Do not depend on this in production yet.

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
- [`scripts`](scripts): quality, coverage, complexity, and release-check helpers
- [`CONTRIBUTING.md`](CONTRIBUTING.md): contributor workflow, including the
  sample/library PR separation rule
- [`CHANGELOG.md`](CHANGELOG.md): release history and unreleased changes
- [`SECURITY.md`](SECURITY.md): vulnerability reporting and project security boundaries
- [`GUIDELINES_NEST_KAFKA.md`](GUIDELINES_NEST_KAFKA.md): the project constitution

Samples and a documentation site are part of the public learning path and arrive
in later milestones.

## Installation

```bash
npm i @nest-native/kafka @confluentinc/kafka-javascript
```

Required peers:

```bash
npm i @nestjs/common @nestjs/core @nestjs/microservices reflect-metadata rxjs
```

## Usage (scaffold)

At this milestone the module only wires global configuration and a feature-module
entry point. The consumer decorators and the producer service are not implemented
yet.

```ts
import { Module } from '@nestjs/common';
import { KafkaModule } from '@nest-native/kafka';

@Module({
  imports: [
    KafkaModule.forRoot({
      clientId: 'orders-service',
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
  }),
});
```

Feature modules register their handler classes through
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

## Quality Gates

The repository ships the same review posture as its sibling `@nest-native`
packages, using `node:test` and `c8`:

- package build, typecheck, and coverage on Node.js 20 and 22
- coverage with `c8`, enforced at 100% for statements, branches, functions, and lines
- sticky PR comments for coverage, test performance, and cognitive complexity
- cognitive complexity enforcement with SonarJS threshold `15`
- package tarball validation and README link validation
- supply-chain audit for high-severity issues

Run the local gate with:

```bash
npm run ci
```

## Status and Roadmap

This is the bootstrap milestone. The planned path:

1. **Bootstrap** — repo skeleton, empty package, CI green (this milestone).
2. `KafkaModule.forRoot()` + producer service. One logging handler. Smoke test against a local Kafka container.
3. `@KafkaConsumer` + `@KafkaHandler` with the full enhancer pipeline. Showcase sample.
4. Header + context parameter decorators. Error mapping. Graceful shutdown.
5. Batch consume + per-topic concurrency. Address `#12703` / `#12355` explicitly.
6. Transactional producer helper.
7. `KafkaTestModule` + mock helpers. Migration guide from `@nestjs/microservices` Kafka.
8. Documentation site. Release `v0.1`.

See [CHANGELOG.md](CHANGELOG.md) for what has landed.

## License

[MIT](LICENSE) © 2026 Rodrigo Nogueira.

Part of the [nest-native](https://github.com/nest-native) family, alongside
[@nest-native/drizzle](https://github.com/nest-native/drizzle) and
[@nest-native/trpc](https://github.com/nest-native/trpc).
