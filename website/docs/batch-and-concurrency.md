# Batch & Concurrency

This page covers batch consumption, per-topic concurrency
([`nestjs/nest#12703`](https://github.com/nestjs/nest/issues/12703)),
rebalance-safe offsets
([`nestjs/nest#12355`](https://github.com/nestjs/nest/issues/12355)), and
backpressure.

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

## Rebalance-Safe Offsets (`#12355`)

Batch consumers resolve each message's offset as the batch is processed — the
client's all-or-nothing auto-resolve is disabled. If a partition is revoked
mid-batch during a rebalance, the consumer keeps the progress already made instead
of replaying the whole batch or hanging. Combined with the rule that offsets
commit only after a successful handler return, in-flight messages either complete
or are explicitly accounted for.

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
