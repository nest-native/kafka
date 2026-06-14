# @nest-native/kafka

<p align="center">Decorator-first NestJS Kafka integration built on Confluent's officially supported @confluentinc/kafka-javascript client.</p>

<p align="center">
  <a href="https://www.npmjs.com/package/@nest-native/kafka"><img src="https://img.shields.io/npm/v/@nest-native/kafka.svg" alt="NPM Version" /></a>
  <a href="https://opensource.org/licenses/MIT"><img src="https://img.shields.io/badge/license-MIT-green.svg" alt="Package License" /></a>
</p>

> [!WARNING]
> **Status: under construction.** Today the module
> (`KafkaModule.forRoot()` / `forRootAsync()` / `forFeature()`), the
> `KafkaProducerService` (`send`, `sendBatch`, `transactional`),
> `@InjectKafkaProducer()`, and the consumer decorators (`@KafkaConsumer`,
> `@KafkaHandler`) with the full Nest enhancer pipeline exist. The parameter
> decorators (`@KafkaMessage`, `@KafkaHeaders`, `@KafkaContext`), error mapping,
> and graceful shutdown land in later milestones. Do not depend on this in
> production yet.

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

Publish messages with the injected `KafkaProducerService`. It connects when the
module initialises and disconnects on graceful shutdown:

```ts
import { Injectable } from '@nestjs/common';
import { KafkaProducerService } from '@nest-native/kafka';

@Injectable()
export class OrdersService {
  constructor(private readonly producer: KafkaProducerService) {}

  async placeOrder(id: string): Promise<void> {
    // Single topic
    await this.producer.send({
      topic: 'orders.placed',
      messages: [{ key: id, value: JSON.stringify({ id }) }],
    });

    // Many topics in one call
    await this.producer.sendBatch({
      topicMessages: [
        { topic: 'orders.placed', messages: [{ value: id }] },
        { topic: 'audit.log', messages: [{ value: `order ${id}` }] },
      ],
    });

    // Transactional: commits on success, aborts on throw
    await this.producer.transactional(async tx => {
      await tx.send({ topic: 'orders.placed', messages: [{ value: id }] });
    });
  }
}
```

For low-level access to the raw Confluent producer, inject it directly with
`@InjectKafkaProducer()`.

### Consuming messages

Mark a class with `@KafkaConsumer` and its methods with `@KafkaHandler`. The
methods run through the full Nest enhancer pipeline — `@UseGuards`,
`@UseInterceptors`, `@UsePipes`, `@UseFilters` — exactly as they do for an HTTP
controller or a `@nestjs/microservices` handler. The parsed payload is the first
argument and the raw `KafkaContext` is the second:

```ts
import { Injectable, UseGuards } from '@nestjs/common';
import { KafkaConsumer, KafkaContext, KafkaHandler } from '@nest-native/kafka';

@Injectable()
@KafkaConsumer('orders.placed', { groupId: 'orders-service' })
@UseGuards(TenantGuard)
export class OrdersConsumer {
  @KafkaHandler()
  handle(order: OrderPlaced, context: KafkaContext): void {
    // runs after guards, interceptors, and pipes; exception filters wrap it
    console.log(`order on ${context.getTopic()}`, order);
  }
}
```

Register the consumer (and any guard/interceptor/pipe/filter classes it uses) as
providers, then list it in `KafkaModule.forFeature([OrdersConsumer])` or directly
in a module's `providers`. Consumers in the same consumer group share a single
Confluent consumer so partitions balance across instances. The payload is
JSON-parsed by default, falling back to the decoded string for non-JSON values;
header conventions stay neutral.

### Testing without a broker

`KafkaModule.forRoot({ driverFactory })` accepts a custom driver factory so unit
tests (and the `01-producer-basics` sample) can run with an in-memory producer
and never touch a real broker or the native `librdkafka` binary.

## Links

- Source and issues: [github.com/nest-native/kafka](https://github.com/nest-native/kafka)
- Changelog: [CHANGELOG.md](../../CHANGELOG.md)
- Confluent client: [confluentinc/confluent-kafka-javascript](https://github.com/confluentinc/confluent-kafka-javascript)
- The nest-native family: [@nest-native/drizzle](https://www.npmjs.com/package/@nest-native/drizzle), [@nest-native/trpc](https://www.npmjs.com/package/@nest-native/trpc)
