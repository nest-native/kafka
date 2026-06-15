# @nest-native/kafka

<p align="center">Decorator-first NestJS Kafka integration built on Confluent's officially supported @confluentinc/kafka-javascript client.</p>

<p align="center">
  <a href="https://www.npmjs.com/package/@nest-native/kafka"><img src="https://img.shields.io/npm/v/@nest-native/kafka.svg" alt="NPM Version" /></a>
  <a href="https://opensource.org/licenses/MIT"><img src="https://img.shields.io/badge/license-MIT-green.svg" alt="Package License" /></a>
</p>

> [!NOTE]
> **Status: `0.1.1` (`0.x`).** Functional and fully tested (100% coverage), and
> usable today — but the public API may still change before `1.0`. Per semver,
> `0.x` minor releases can include breaking changes, so pin a version. See the
> [support policy](https://nest-native.dev/kafka/support-policy). The initial
> `0.x` release covers the module
> (`KafkaModule.forRoot()` / `forRootAsync()` / `forFeature()`), the
> `KafkaProducerService` (`send`, `sendBatch`, `transactional`),
> `@InjectKafkaProducer()`, the consumer decorators (`@KafkaConsumer`,
> `@KafkaHandler`) with the full Nest enhancer pipeline, the parameter decorators
> (`@KafkaMessage`, `@KafkaHeaders`, `@KafkaCtx`, `@KafkaBatch`), error mapping,
> graceful shutdown, batch consumption, per-topic concurrency, backpressure, and
> the testing utilities (`KafkaTestModule`, `InMemoryKafkaBroker`,
> `createMockKafkaProducer`). The [documentation site](https://nest-native.github.io/kafka/)
> covers the whole surface.

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

### Transactions

`transactional(work)` runs `work` inside one Kafka transaction: it commits when
`work` resolves and aborts — delivering nothing — when it throws, re-raising the
original error. Configure a `transactionalId` to make the shared producer
transactional (Confluent's client then also enables idempotence):

```ts
KafkaModule.forRoot({
  client: { brokers: ['localhost:9092'] },
  producer: { transactionalId: 'orders-producer' }, // unique per producer instance
});
```

```ts
// Atomic across topics: both writes land, or neither does.
await this.producer.transactional(async tx => {
  await tx.send({ topic: 'orders.placed', messages: [{ value: id }] });
  await tx.sendBatch({
    topicMessages: [{ topic: 'orders.audit', messages: [{ value: `placed ${id}` }] }],
  });
});
```

For the consume-process-produce ("read-process-write") pattern, commit the
consumer's offset inside the same transaction with `sendOffsets`, so the produced
message and the consumed offset commit atomically — exactly-once across the
consume → produce step:

```ts
await this.producer.transactional(async tx => {
  await tx.send({ topic: 'receipts.issued', messages: [{ value: receipt }] });
  await tx.sendOffsets({
    consumer, // the live consumer object (see migration note below)
    topics: [
      {
        topic: 'payments.captured',
        // commit "next offset to read" = consumed offset + 1
        partitions: [{ partition, offset: String(Number(offset) + 1) }],
      },
    ],
  });
});
```

> **Migration note (kafkajs → Confluent).** `sendOffsets` takes the live
> `consumer` object in `@confluentinc/kafka-javascript`, not the
> `consumerGroupId` string kafkajs used. The package's `KafkaTransactionOffsets`
> type models the Confluent shape.

If the abort itself fails while unwinding a failed `work`, the original error
still surfaces with the abort failure attached as its `cause`, so neither error
is lost. The package wraps only what the client provides — it does not add
exactly-once helpers beyond Confluent's transactions.

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

### Parameter decorators

Instead of the positional `(payload, context)` arguments you can decorate
individual parameters, mirroring `@Payload()` / `@Ctx()` from
`@nestjs/microservices`. The decorators participate in the enhancer pipeline, so
param-level pipes run just as they do on an HTTP controller argument:

```ts
import { ParseIntPipe } from '@nestjs/common';
import {
  KafkaConsumer,
  KafkaContext,
  KafkaCtx,
  KafkaHandler,
  KafkaHeaders,
  KafkaMessage,
  KafkaMessageHeaders,
} from '@nest-native/kafka';

@KafkaConsumer('orders.placed')
export class OrdersConsumer {
  @KafkaHandler()
  handle(
    @KafkaMessage() order: OrderPlaced, // whole parsed payload
    @KafkaMessage('id') id: string, // one payload property
    @KafkaHeaders() headers: KafkaMessageHeaders, // all headers (empty if none)
    @KafkaHeaders('trace-id') traceId: string | Buffer, // one header by key
    @KafkaCtx() context: KafkaContext, // topic, partition, raw message, headers
  ): void {}
}
```

### Error mapping

When a handler throws and no `@UseFilters` exception filter handles it, the
transport maps the error to consumer behaviour instead of swallowing it
(`nestjs/nest#9679`):

- A 4xx `HttpException` (e.g. `BadRequestException`) is a non-retryable client
  error, so the offset is committed — a poison message is acknowledged instead of
  redelivered forever.
- Any other error (a 5xx `HttpException`, an `RpcException`, or an arbitrary
  thrown value) is treated as transient and retried: the offset is left
  uncommitted so the broker redelivers.

Override the policy with your own mapper — for example to route a failure to a
dead-letter topic before committing:

```ts
KafkaModule.forRoot({
  client: { brokers: ['localhost:9092'] },
  errorMapper: (error, context) => (isFatal(error) ? 'commit' : 'retry'),
});
```

### Batch consumption and per-topic concurrency

Opt a handler into batch mode to process a whole fetched topic-partition batch at
once instead of one message at a time. `@KafkaMessage()` then resolves to the
array of deserialized payloads, and `@KafkaBatch()` resolves to the raw
`KafkaConsumerBatch` (topic, partition, original messages with keys, headers, and
offsets):

```ts
@KafkaConsumer('metrics', { groupId: 'aggregator', concurrency: 2 })
export class MetricsConsumer {
  @KafkaHandler(undefined, { batch: true }) // inherits the consumer's topic
  aggregate(
    @KafkaMessage() metrics: Metric[],
    @KafkaBatch() batch: KafkaConsumerBatch,
  ) {
    // runs once per fetched batch; batch.partition is the source partition
  }
}
```

- **Per-topic concurrency (`nestjs/nest#12703`).** `concurrency` sets the
  consumer's `partitionsConsumedConcurrently`. The default is `1` (strict
  per-partition ordering); raising it processes partitions concurrently while
  preserving order within each partition. Resolution is handler → consumer →
  `KafkaModule.forRoot({ concurrency })` → `1`.
- **Rebalance safety (`nestjs/nest#12355`).** Batch consumers resolve each
  message's offset as the batch is processed (the client's all-or-nothing
  auto-resolve is disabled), so a partition revoked mid-batch keeps the progress
  already made instead of replaying the whole batch or hanging.
- **Backpressure.** `maxInFlight` caps how many messages/batches a consumer
  processes at once, so a fast broker cannot overwhelm slow handlers. The default
  is uncapped (`0`); it resolves handler → consumer → module the same way as
  `concurrency`.
- Per-message and batch handlers in the same group always run on separate Kafka
  consumers, because a consumer runs either `eachMessage` or `eachBatch`.

### Graceful shutdown

On `app.close()` the transport stops accepting newly delivered messages, drains
the messages (and batches) already in flight so no handler is interrupted
mid-message, then disconnects every consumer. Enable Nest's shutdown hooks
(`app.enableShutdownHooks()`) for it to run on `SIGTERM`/`SIGINT`.

### Testing without a broker

Use `KafkaTestModule` in place of `KafkaModule` to run the whole transport —
producer service, the `@KafkaConsumer` pipeline, batch consumption, transactions,
graceful shutdown — against an in-memory `InMemoryKafkaBroker`. No real broker, no
native `librdkafka`, no `KAFKA_BROKERS` env required:

```ts
import { Test } from '@nestjs/testing';
import {
  InMemoryKafkaBroker,
  KAFKA_TEST_BROKER,
  KafkaTestModule,
} from '@nest-native/kafka';

const moduleRef = await Test.createTestingModule({
  imports: [KafkaTestModule.forRoot(), OrdersModule],
}).compile();
await moduleRef.init(); // fires onApplicationBootstrap; consumers subscribe

const broker = moduleRef.get<InMemoryKafkaBroker>(KAFKA_TEST_BROKER);

// Inject a message straight to a consumer...
await broker.emit('orders.placed', { value: JSON.stringify({ id: '1' }) });
// ...or produce through a service and assert on what the broker recorded:
expect(broker.getSentTo('orders.placed')).toHaveLength(1);

await moduleRef.close();
```

`KafkaTestModule.forRoot(options?)` / `forRootAsync(options?)` accept the same
options as `KafkaModule` (minus `driverFactory`, which is fixed to the in-memory
broker) and a `broker` option to reuse an existing `InMemoryKafkaBroker`. Inject
the broker with the `KAFKA_TEST_BROKER` token or `@InjectKafkaTestBroker()`.

For a unit test of a service that injects the producer (no Nest module needed),
reach for `createMockKafkaProducer()`, a recording mock `KafkaDriverProducer`:

```ts
import { createMockKafkaProducer, KafkaProducerService } from '@nest-native/kafka';

const { producer, calls } = createMockKafkaProducer();
const service = new KafkaProducerService(producer);
await service.send({ topic: 'orders', messages: [{ value: 'hi' }] });
expect(calls.send).toHaveLength(1);
```

For low-level control, `KafkaModule.forRoot({ driverFactory })` still accepts any
custom driver factory.

### Migrating from `@nestjs/microservices` Kafka

Porting an app off the official Kafka transport is mostly a mechanical rename
(`@Controller`/`@EventPattern`/`@Payload()` → `@KafkaConsumer`/`@KafkaHandler`/
`@KafkaMessage()`), plus a few behavioural deltas (explicit serialization,
exception mapping, the Confluent `sendOffsets` shape). The full field-by-field
guide is in [docs/migration-from-nestjs-microservices.md](../../docs/migration-from-nestjs-microservices.md),
validated end-to-end by `sample/06-microservice-migration`.

## Links

- Source and issues: [github.com/nest-native/kafka](https://github.com/nest-native/kafka)
- Changelog: [CHANGELOG.md](../../CHANGELOG.md)
- Confluent client: [confluentinc/confluent-kafka-javascript](https://github.com/confluentinc/confluent-kafka-javascript)
- The nest-native family: [@nest-native/drizzle](https://www.npmjs.com/package/@nest-native/drizzle), [@nest-native/trpc](https://www.npmjs.com/package/@nest-native/trpc)
