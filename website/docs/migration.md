# Migration Guide

This guide ports a Kafka application off `@nestjs/microservices`'s official Kafka
transport (built on `kafkajs`) onto `@nest-native/kafka` (built on Confluent's
officially supported `@confluentinc/kafka-javascript`). The migration is mostly a
mechanical rename: handler bodies, DI, and the Nest enhancer pipeline carry over
unchanged. The behavioral deltas — the parts that are *not* a rename — are called
out explicitly.

A runnable end-to-end version lives in `sample/06-microservice-migration`, whose
smoke test proves the ported consumer works with `KafkaTestModule`. The canonical,
field-by-field guide is kept in the repository at
[docs/migration-from-nestjs-microservices.md](https://github.com/nest-native/kafka/blob/main/docs/migration-from-nestjs-microservices.md).

## At A Glance

| `@nestjs/microservices` Kafka | `@nest-native/kafka` |
| --- | --- |
| `ClientsModule.register([{ transport: Transport.KAFKA, ... }])` | `KafkaModule.forRoot({ ... })` / `forRootAsync` |
| `@Controller()` on the consumer class | `@KafkaConsumer('topic'?, options?)` |
| `@MessagePattern('topic')` / `@EventPattern('topic')` | `@KafkaHandler('topic'?, options?)` |
| `@Payload()` | `@KafkaMessage()` |
| `@Ctx() ctx: KafkaContext` | `@KafkaCtx() ctx: KafkaContext` |
| (read headers off the raw message) | `@KafkaHeaders()` |
| `ClientKafka` + `client.emit()` / `client.send()` | `KafkaProducerService` or `@InjectKafkaProducer()` |
| `app.connectMicroservice(...)` + `app.startAllMicroservices()` | nothing — consumers start on application bootstrap |
| Custom test harness / real broker | `KafkaTestModule` + `createMockKafkaProducer()` |

`@UseGuards`, `@UseInterceptors`, `@UsePipes`, and `@UseFilters` work exactly as
before, and handlers still run under the `'rpc'` execution-context type.

## The Consumer Class

Before:

```ts
import {Controller} from '@nestjs/common';
import {Ctx, EventPattern, KafkaContext, Payload} from '@nestjs/microservices';

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
import {Injectable} from '@nestjs/common';
import {KafkaConsumer, KafkaContext, KafkaCtx, KafkaHandler, KafkaMessage} from '@nest-native/kafka';

@Injectable()
@KafkaConsumer('orders.placed', {groupId: 'orders-consumer'})
export class OrdersConsumer {
  @KafkaHandler()
  handleOrderPlaced(@KafkaMessage() order: OrderPlaced, @KafkaCtx() context: KafkaContext) {
    // identical body
  }
}
```

The consumer group moves from the transport options onto `@KafkaConsumer` (or
`@KafkaHandler`). There is no separate "start the microservice" step — the
consumer explorer subscribes during `onApplicationBootstrap`. See
[Module](module.md) and [Consumers](consumers.md).

## `@MessagePattern` vs `@EventPattern`

`@nest-native/kafka` models Kafka as the event log it is: `@KafkaHandler` is
fire-and-forget, like `@EventPattern`. If you relied on the transport's built-in
request/reply correlation, implement it explicitly by producing to a reply topic
with `KafkaProducerService` and correlating with a header you own — the package
stays neutral on header keys.

## Producing Messages

Before, you injected a `ClientKafka` and called `client.emit(topic, object)`.
After, inject `KafkaProducerService`:

```ts
constructor(private readonly producer: KafkaProducerService) {}

await this.producer.send({
  topic: 'orders.placed',
  messages: [{key: order.id, value: JSON.stringify(order)}],
});
```

Key delta: **serialization is explicit.** The official transport serialized
objects for you; here the message `value` is a string / Buffer / `null`. On the
way in, the consumer JSON-parses by default and falls back to the decoded string.
See [Producer](producer.md).

## Behavioral Deltas

These are the parts that are *not* a rename.

- **Exception handling ([`#9679`](https://github.com/nestjs/nest/issues/9679)).**
  Unhandled handler errors map to commit/retry instead of being swallowed. See
  [Error Mapping](error-mapping.md).
- **Sequential per-topic processing ([`#12703`](https://github.com/nestjs/nest/issues/12703)).**
  `concurrency` sets `partitionsConsumedConcurrently`; default `1`. See
  [Batch & Concurrency](batch-and-concurrency.md).
- **Rebalance safety ([`#12355`](https://github.com/nestjs/nest/issues/12355)).**
  Batch consumers resolve each offset as it is processed. See
  [Batch & Concurrency](batch-and-concurrency.md).
- **`sendOffsets` shape.** Takes the live consumer object, not a
  `consumerGroupId` string. See [Transactions](transactions.md).
- **Backpressure.** `maxInFlight` caps in-flight work; default uncapped.

## Testing

Replace any real-broker or hand-rolled test harness with `KafkaTestModule`, and
unit-test producer-injecting services with `createMockKafkaProducer()`. See
[Testing](testing.md).
