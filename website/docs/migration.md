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
| `@EventPattern('topic')` | `@KafkaHandler('topic'?, options?)` |
| `@MessagePattern('topic')` | `@KafkaHandler('topic'?, {reply: true})` |
| `client.send('topic', value)` | `KafkaRequestReplyService.request()` |
| `client.subscribeToResponseOf('topic')` | nothing — delete it |
| `@Payload()` | `@KafkaMessage()` |
| `@Ctx() ctx: KafkaContext` | `@KafkaCtx() ctx: KafkaContext` |
| (read headers off the raw message) | `@KafkaHeaders()` |
| `ClientKafka` + `client.emit()` | `KafkaProducerService` or `@InjectKafkaProducer()` |
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

`@nest-native/kafka` models Kafka as the event log it is, so `@KafkaHandler` is
fire-and-forget by default — the direct equivalent of `@EventPattern`.

`@MessagePattern` is request/reply, and it ports too: add `reply: true` and the
handler's return value becomes the reply, addressed by the request's own
headers.

```ts
@KafkaHandler('orders.total', {reply: true})
async total(@KafkaMessage() query: TotalQuery): Promise<TotalResult> {
  return this.orders.total(query.customerId); // this becomes the reply
}
```

The calling side replaces `client.send()` with `KafkaRequestReplyService`, and
`subscribeToResponseOf()` disappears entirely:

```ts
KafkaModule.forRoot({
  client: {brokers: ['localhost:9092']},
  requestReply: {replyTopic: 'orders-api.replies'},
});

const reply = await this.requests.request<TotalResult>({
  topic: 'orders.total',
  message: {value: JSON.stringify({customerId})},
});
```

Three things are worth knowing before you lean on it, and all of them are in
[Request-Reply](request-reply.md):

- The header contract defaults to the official transport's own keys, so a
  **partially migrated fleet interoperates in both directions** with no
  configuration on either side. Migrate one service at a time.
- The reply path is **at-most-once**, and a timeout means the outcome is
  *unknown*, never "it did not happen". There is a 30s default timeout here; the
  official client's `send()` waits forever unless you added your own `timeout`
  operator.
- Reply routing costs **N-times fan-out** across your replicas. The page states
  the arithmetic and says plainly when a Kafka round trip is the wrong tool.

Earlier versions of this guide told you to hand-roll correlation with a header
you own. That advice was wrong at scale: correlating is the easy half, and
making the reply reach the *instance* that asked — across rebalances, restarts,
and N replicas — is the half that is unsafe to build yourself. That is now the
package's job.

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
- **Request-reply error timing.** The official transport error-replies on *every*
  handler failure. Here a `'retry'`-mapped error (the default for non-4xx) is
  redelivered server-side first, so an un-migrated caller's fast failure becomes
  a timeout unless a retry succeeds in the window. One `errorMapper` line
  returning `'commit'` for those topics restores the old behaviour. See
  [Request-Reply](request-reply.md).

## Testing

Replace any real-broker or hand-rolled test harness with `KafkaTestModule`, and
unit-test producer-injecting services with `createMockKafkaProducer()`. See
[Testing](testing.md).
