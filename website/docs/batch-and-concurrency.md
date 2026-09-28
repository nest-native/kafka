# Batch & Concurrency

This page covers batch consumption, per-topic concurrency
([`nestjs/nest#12703`](https://github.com/nestjs/nest/issues/12703)),
batch offsets and redelivery, and backpressure.

## Batch Consumption

Opt a handler into batch mode to process a whole fetched topic-partition batch at
once instead of one message at a time. `@KafkaMessage()` then resolves to the
array of deserialized payloads, and `@KafkaBatch()` resolves to the raw
`KafkaConsumerBatch`:

```ts
import {KafkaBatch, KafkaConsumer, KafkaConsumerBatch, KafkaHandler, KafkaMessage} from '@nest-native/kafka';

@KafkaConsumer('metrics', {groupId: 'aggregator', concurrency: 2})
export class MetricsConsumer {
  @KafkaHandler(undefined, {batch: true}) // inherits the consumer's topic
  aggregate(
    @KafkaMessage() metrics: Metric[],
    @KafkaBatch() batch: KafkaConsumerBatch,
  ) {
    // runs once per fetched batch; batch.partition is the source partition
  }
}
```

Per-message and batch handlers in the same group always run on separate Kafka
consumers, because a consumer runs either `eachMessage` or `eachBatch`.

## Per-Topic Concurrency (`#12703`)

The official transport processes a topic sequentially. Here, the `concurrency`
option sets the consumer's `partitionsConsumedConcurrently`:

- The default is `1` — strict per-partition ordering.
- Raising it processes partitions concurrently while preserving order **within**
  each partition.
- Resolution is handler → consumer → `KafkaModule.forRoot({concurrency})` → `1`,
  so a single handler can opt in or out of the module-wide default.

```ts
KafkaModule.forRoot({
  client: {brokers: ['localhost:9092']},
  concurrency: 4, // module-wide default
});
```

## Batch Offsets and Redelivery

A batch is committed only after it has been handled. The transport resolves a
batch's offsets once every handler routed to its topic has returned — or has
failed with an error the [error mapper](error-mapping.md) maps to `'commit'`. A
failure mapped to `'retry'` leaves them unresolved, so the client seeks back to
the batch's first message and the broker hands the whole batch back.

The batch is the unit of work. A handler receives every message at once, so the
transport cannot know which of them the handler finished before it failed, and
it never claims more than the handler reported. Batch consumption is therefore
at-least-once, like per-message consumption: a redelivered batch runs again in
full — including messages a failed attempt had already processed — and so does
a batch whose partition was revoked mid-flight and reassigned elsewhere. Make
batch handlers idempotent.

Versions up to 0.5.1 resolved the offsets while the batch was being decoded,
before the handler ran, so a batch that failed with `'retry'` was committed and
never redelivered. The real-broker suite now proves the redelivery.

Rebalances themselves are the client's business.
[`nestjs/nest#12355`](https://github.com/nestjs/nest/issues/12355) was a
rebalance that never settled, caused by the official transport's custom
reply-partition assigner. This package uses the client's standard assignors —
request-reply included, which consumes replies through a single-member group
per instance — so that loop cannot form.

## Backpressure

`maxInFlight` caps how many messages or batches a consumer processes at once, so a
fast broker cannot overwhelm slow handlers:

- The default is uncapped (`0`).
- It resolves handler → consumer → module, the same way as `concurrency`.

```ts
@KafkaConsumer('metrics', {groupId: 'aggregator', maxInFlight: 100})
export class MetricsConsumer {}
```

## Sample

Sample `04-batch-concurrency` demonstrates batch consume, per-topic concurrency,
and rebalance-safe offset resolution. See the [Sample Catalog](samples/catalog.md).
