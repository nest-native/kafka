# Sample 04 — Batch consume and per-topic concurrency

Demonstrates milestone 5: batch consumption and per-topic concurrency, the two
correctness gaps the official `@nestjs/microservices` Kafka transport left open.

What it shows:

- **Batch consumption.** A `@KafkaHandler(topic, { batch: true })` runs once per
  fetched topic-partition batch instead of once per message. `@KafkaMessage()`
  resolves to the array of deserialized payloads; `@KafkaBatch()` resolves to the
  raw `KafkaConsumerBatch` (topic, partition, original messages with their keys,
  headers, and offsets).
- **Per-topic concurrency (`nestjs/nest#12703`).** The official transport
  processes a topic's partitions sequentially. Here `@KafkaConsumer(topic, {
  concurrency: 2 })` sets `partitionsConsumedConcurrently`, so two partitions
  aggregate at the same time. Ordering *within* a partition is always preserved.
  The default is `1` (strict ordering); raise it to opt in. Resolution is
  handler → consumer → `KafkaModule.forRoot({ concurrency })` → `1`.
- **Rebalance safety (`nestjs/nest#12355`).** The transport resolves each
  message's offset as the batch is processed (it disables the client's
  all-or-nothing `eachBatchAutoResolve`). A partition revoked mid-batch keeps the
  offsets already resolved, so the next owner resumes after the last processed
  message instead of replaying the whole batch or hanging.
- **Backpressure.** `KafkaModule.forRoot({ maxInFlight })` (or the per-consumer /
  per-handler override) caps how many batches a consumer processes at once, so a
  fast broker cannot overwhelm slow handlers. The default is uncapped.
- **Graceful shutdown** drains in-flight batches before disconnecting.

The smoke test ingests a window across two partitions and asserts: one handler
invocation per partition batch, the per-partition aggregates, and that every
message offset was resolved (rebalance safety).

## Run it

```bash
# in-memory loopback broker, no Kafka required
npm run test --workspace nest-native-kafka-sample-04-batch-concurrency
npm run start --workspace nest-native-kafka-sample-04-batch-concurrency
```

## Against a real broker

```bash
KAFKA_BROKERS=localhost:9092 \
  npm run start --workspace nest-native-kafka-sample-04-batch-concurrency
```

Setting `KAFKA_BROKERS` switches to Confluent's `@confluentinc/kafka-javascript`
client. Broker credentials must never be committed to sample code, logs, or docs.
