# @nest-native/kafka

<p align="center">Decorator-first NestJS Kafka integration built on Confluent's officially supported @confluentinc/kafka-javascript client.</p>

<p align="center">
  <a href="https://www.npmjs.com/package/@nest-native/kafka"><img src="https://img.shields.io/npm/v/@nest-native/kafka.svg" alt="NPM Version" /></a>
  <a href="https://opensource.org/licenses/MIT"><img src="https://img.shields.io/badge/license-MIT-green.svg" alt="Package License" /></a>
</p>

> [!WARNING]
> **Status: scaffold / under construction.** This is the `v0.0.1-scaffold`
> bootstrap. Only `KafkaModule.forRoot()` / `KafkaModule.forRootAsync()` /
> `KafkaModule.forFeature()` exist today. The consumer decorators
> (`@KafkaConsumer`, `@KafkaHandler`), the parameter decorators
> (`@KafkaMessage`, `@KafkaHeaders`, `@KafkaContext`), and `KafkaProducerService`
> land in later milestones. Do not depend on this in production yet.

## What This Is

`@nest-native/kafka` is a community NestJS integration that will make Kafka
consumers and producers feel like a first-class Nest transport — preserving the
`@MessagePattern` / `@EventPattern` ergonomics of `@nestjs/microservices` while
solving the correctness gaps the kafkajs-based official transport accumulated
(sequential per-topic processing, rebalance hangs, exception swallowing).

The headline goal: a decorator-first transport built on Confluent's officially
supported `@confluentinc/kafka-javascript` client, with the full Nest enhancer
pipeline (guards, pipes, interceptors, filters) intact on handler methods.

## Compatibility

| Runtime | Supported line |
| --- | --- |
| Node.js | `>=20` |
| NestJS | `11.x` |
| `@confluentinc/kafka-javascript` | `^1.9` (pin major; tracks librdkafka) |
| Validation | class-validator and Zod, both app-owned |

The published package has no runtime dependencies. The Confluent client and the
NestJS packages are declared as `peerDependencies`, so applications install only
the ecosystems they actually use.

## Installation

```bash
npm i @nest-native/kafka @confluentinc/kafka-javascript
```

Required peers:

```bash
npm i @nestjs/common @nestjs/core @nestjs/microservices reflect-metadata rxjs
```

## Usage (scaffold)

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

## Links

- Source and issues: [github.com/nest-native/kafka](https://github.com/nest-native/kafka)
- Changelog: [CHANGELOG.md](../../CHANGELOG.md)
- Confluent client: [confluentinc/confluent-kafka-javascript](https://github.com/confluentinc/confluent-kafka-javascript)
- The nest-native family: [@nest-native/drizzle](https://www.npmjs.com/package/@nest-native/drizzle), [@nest-native/trpc](https://www.npmjs.com/package/@nest-native/trpc)
