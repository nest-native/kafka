# Migrating from `@nestjs/microservices` Kafka

This guide ports a Kafka application off `@nestjs/microservices`'s official Kafka
transport (built on `kafkajs`) onto `@nest-native/kafka` (built on Confluent's
officially supported `@confluentinc/kafka-javascript`). The migration is mostly a
mechanical rename: handler bodies, DI, and the Nest enhancer pipeline carry over
unchanged. The behavioural deltas — the parts that are *not* a rename — are
called out explicitly.

A runnable end-to-end version of this guide lives in
[`sample/06-microservice-migration`](../sample/06-microservice-migration), whose
smoke test proves the ported consumer works using `KafkaTestModule`.

## At a glance

| `@nestjs/microservices` Kafka | `@nest-native/kafka` |
| --- | --- |
| `ClientsModule.register([{ transport: Transport.KAFKA, ... }])` | `KafkaModule.forRoot({ ... })` / `forRootAsync` |
| `@Controller()` on the consumer class | `@KafkaConsumer('topic'?, options?)` |
| `@MessagePattern('topic')` / `@EventPattern('topic')` | `@KafkaHandler('topic'?, options?)` |
| `@Payload()` | `@KafkaMessage()` |
| `@Ctx() ctx: KafkaContext` | `@KafkaCtx() ctx: KafkaContext` |
| (read headers off the raw message) | `@KafkaHeaders()` |
| `ClientKafka` + `client.emit()` / `client.send()` | `KafkaProducerService` (`send` / `sendBatch` / `transactional`) or `@InjectKafkaProducer()` |
| `app.connectMicroservice(...)` + `app.startAllMicroservices()` | nothing — consumers start on application bootstrap |
| Custom test harness / real broker | `KafkaTestModule` (in-memory) + `createMockKafkaProducer()` |

`@UseGuards`, `@UseInterceptors`, `@UsePipes`, and `@UseFilters` work exactly as
before, and handlers still run under the `'rpc'` execution-context type, so any
enhancer that branches on `host.getType()` keeps working.

## 1. Module setup

Before — register the transport and start the microservice:

```ts
import { Transport } from '@nestjs/microservices';

const app = await NestFactory.create(AppModule);
app.connectMicroservice({
  transport: Transport.KAFKA,
  options: {
    client: { clientId: 'orders-service', brokers: ['localhost:9092'] },
    consumer: { groupId: 'orders-consumer' },
  },
});
await app.startAllMicroservices();
await app.listen(3000);
```

After — import the module; consumers start automatically on bootstrap:

```ts
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

The consumer group is declared per consumer (or per handler) instead of once on
the transport — see the next section. There is no separate "start the
microservice" step: the consumer explorer subscribes during
`onApplicationBootstrap`, so a plain `NestFactory.create` /
`createApplicationContext` is enough. Enable `app.enableShutdownHooks()` so
graceful shutdown drains in-flight handlers on `SIGTERM`/`SIGINT`.

## 2. The consumer class

Before:

```ts
import { Controller } from '@nestjs/common';
import { Ctx, EventPattern, KafkaContext, Payload } from '@nestjs/microservices';

@Controller()
export class OrdersController {
  @EventPattern('orders.placed')
  handleOrderPlaced(@Payload() order: OrderPlaced, @Ctx() context: KafkaContext) {
    // ...
  }
}
```

After:

```ts
import { Injectable } from '@nestjs/common';
import {
  KafkaConsumer,
  KafkaContext,
  KafkaCtx,
  KafkaHandler,
  KafkaMessage,
} from '@nest-native/kafka';

@Injectable()
@KafkaConsumer('orders.placed', { groupId: 'orders-consumer' })
export class OrdersConsumer {
  @KafkaHandler()
  handleOrderPlaced(@KafkaMessage() order: OrderPlaced, @KafkaCtx() context: KafkaContext) {
    // identical body
  }
}
```

Notes:

- The consumer group moves from the transport options onto `@KafkaConsumer` (or
  `@KafkaHandler`, which overrides the class). Handlers that share a group share a
  single Confluent consumer, so partitions balance across instances — same as the
  official transport.
- `@KafkaConsumer`'s topic is a default for its handlers; pass a topic to
  `@KafkaHandler('other.topic')` to override or to host several topics on one
  class.
- Register the consumer (and any guard/interceptor/pipe/filter classes) as
  providers, via a module's `providers` or `KafkaModule.forFeature([OrdersConsumer])`.
- `KafkaContext` mirrors the official transport's accessors —
  `getTopic()`, `getPartition()`, `getMessage()` — so context-reading code needs
  no changes.

### `@MessagePattern` vs `@EventPattern`

The official transport distinguishes request/response (`@MessagePattern`, which
auto-replies to a reply topic) from fire-and-forget (`@EventPattern`).
`@nest-native/kafka` models Kafka as the event log it is: `@KafkaHandler` is
fire-and-forget, like `@EventPattern`. If you relied on the transport's built-in
request/reply correlation, implement it explicitly by producing to a reply topic
with `KafkaProducerService` and correlating with a header you own (the package
stays neutral on header keys).

## 3. Parameter decorators

| Before | After |
| --- | --- |
| `@Payload() body` | `@KafkaMessage() body` |
| `@Payload('id') id` | `@KafkaMessage('id') id` |
| `@Ctx() ctx` | `@KafkaCtx() ctx` |
| `ctx.getMessage().headers` | `@KafkaHeaders()` / `@KafkaHeaders('key')` |

All four are built with Nest's public `createParamDecorator`, so param-level
pipes (`@KafkaMessage(ValidationPipe)`, `@KafkaMessage('id', ParseIntPipe)`)
behave exactly as on an HTTP controller argument. You can also keep the
positional `(payload, context)` signature with no parameter decorators at all.

## 4. Producing messages

Before, you injected a `ClientKafka`:

```ts
constructor(@Inject('KAFKA') private readonly client: ClientKafka) {}

await this.client.emit('orders.placed', order); // fire-and-forget
```

After, inject `KafkaProducerService`:

```ts
constructor(private readonly producer: KafkaProducerService) {}

await this.producer.send({
  topic: 'orders.placed',
  messages: [{ key: order.id, value: JSON.stringify(order) }],
});
```

Deltas to be aware of:

- **Explicit serialization.** The official transport serializes objects for you;
  here the message `value` is a string/Buffer/`null`, so serialize explicitly
  (`JSON.stringify`) on the way out. On the way in, the consumer JSON-parses by
  default and falls back to the decoded string for non-JSON values.
- **Connection lifecycle.** `KafkaProducerService` connects on module init and
  disconnects on graceful shutdown; you do not call `connect()` yourself (though
  it is idempotent if you do).
- **Batch and transactions.** Use `sendBatch` for multi-topic writes and
  `transactional(work)` for atomic produce / consume-process-produce. For raw
  Confluent producer access, inject it with `@InjectKafkaProducer()`.

## 5. Behavioural deltas (read this carefully)

These are the parts that are *not* a rename.

### Exception handling (`nestjs/nest#9679`)

The official transport could swallow handler exceptions. Here, when a handler
throws and no `@UseFilters` filter handles it, the error is **mapped to consumer
behaviour** instead of being dropped:

- A 4xx `HttpException` (e.g. `BadRequestException`) is treated as a
  non-retryable client error: the offset is committed so a poison message is not
  redelivered forever.
- Anything else (a 5xx `HttpException`, an `RpcException`, or an arbitrary thrown
  value) is treated as transient and retried: the offset is left uncommitted so
  the broker redelivers.

Override the policy with `KafkaModule.forRoot({ errorMapper })` — for example to
route a failure to a dead-letter topic before committing.

### Sequential per-topic processing (`nestjs/nest#12703`)

The official transport processed a topic's partitions sequentially. Here,
`concurrency` (on the module, `@KafkaConsumer`, or `@KafkaHandler`) sets the
consumer's `partitionsConsumedConcurrently`. The default is `1` (strict
per-partition ordering, matching the old behaviour); raise it to process
partitions concurrently while preserving order within each partition.

### Rebalance safety (`nestjs/nest#12355`)

Batch consumers (`@KafkaHandler(topic?, { batch: true })`) resolve each message's
offset as it is processed, with the client's all-or-nothing auto-resolve
disabled, so a partition revoked mid-batch keeps the progress already made
instead of replaying the whole batch or hanging.

### Transactions: `sendOffsets` (`kafkajs` → Confluent)

For the consume-process-produce pattern, `sendOffsets` takes the **live consumer
object** in `@confluentinc/kafka-javascript`, not the `consumerGroupId` string
`kafkajs` used. `@nest-native/kafka`'s `KafkaTransactionOffsets` type models the
Confluent shape and forwards it untouched. Commit "next offset to read" =
consumed offset + 1.

### Backpressure

`maxInFlight` (module / consumer / handler) caps how many messages or batches a
consumer processes at once. The default is uncapped (`0`).

## 6. Testing

Replace any real-broker or hand-rolled test harness with `KafkaTestModule`, which
runs the entire transport — producer service, the `@KafkaConsumer` pipeline,
graceful shutdown — against an in-memory broker:

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
await moduleRef.init();

const broker = moduleRef.get<InMemoryKafkaBroker>(KAFKA_TEST_BROKER);

// Drive the consumer by producing, or inject a message directly:
await broker.emit('orders.placed', { value: JSON.stringify({ id: '1' }) });

// Assert on what handlers produced:
expect(broker.getSentTo('receipts.issued')).toHaveLength(1);

await moduleRef.close();
```

For a unit test of a service that injects the producer (no module needed), use
the mock helper:

```ts
import { createMockKafkaProducer, KafkaProducerService } from '@nest-native/kafka';

const { producer, calls } = createMockKafkaProducer();
const service = new KafkaProducerService(producer);
await service.send({ topic: 'orders', messages: [{ value: 'hi' }] });
expect(calls.send).toHaveLength(1);
```

## 7. Driver-backed integration

`@nest-native/kafka` lists `@confluentinc/kafka-javascript` as an **optional**
peer, so it is only loaded when you open a real connection. Keep unit tests on
`KafkaTestModule` / mocks (no broker, no native `librdkafka`), and gate
driver-backed integration tests on a `KAFKA_BROKERS` env var so they run in CI
against a real Kafka and skip locally when the env is missing — the same pattern
the samples use.
