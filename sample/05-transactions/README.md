# Sample 05 — Transactional producer

Demonstrates milestone 6: the transactional producer helper
(`KafkaProducerService.transactional`). The callback runs inside one Kafka
transaction — the helper commits when it returns and aborts when it throws, so a
group of writes is atomic across topics.

What it shows:

- **Atomic multi-topic write.** `placeOrder` sends to `orders.placed` and
  `orders.audit` inside one transaction. Both land together on commit.
- **Abort on throw.** `rejectOrder` stages a write and then throws. The helper
  aborts the transaction (so nothing is delivered) and re-throws the original
  error to the caller. If the abort itself fails, the original error still
  surfaces with the abort error attached as its `cause`.
- **Consume-process-produce (`sendOffsets`).** `issueReceipt` produces a receipt
  and commits the source consumer's offset in the *same* transaction via
  `tx.sendOffsets(...)`. The produced message and the consumed offset either both
  commit or neither does — exactly-once across the consume → produce step. The
  offset committed is the consumed offset **+ 1** ("next offset to read").
- **Transactional producer config.** `KafkaModule.forRoot({ producer: {
  transactionalId } })` turns the shared producer transactional; Confluent's
  client then also enables idempotence automatically.

### Migration note (kafkajs → Confluent)

`sendOffsets` differs between clients. kafkajs took a `consumerGroupId` string;
Confluent's `@confluentinc/kafka-javascript` takes the live `consumer` object
instead, so it can fence the group's transaction correctly. This sample (and the
package's `KafkaTransactionOffsets` type) model the Confluent shape:

```ts
await tx.sendOffsets({
  consumer, // the live consumer object — NOT a consumerGroupId string
  topics: [{ topic, partitions: [{ partition, offset: String(consumed + 1) }] }],
});
```

The smoke test asserts: the committed transaction delivered both topics, the
aborted transaction delivered nothing and re-threw, and the consume-process-
produce step issued exactly one receipt while committing the expected offset.

## Run it

```bash
# in-memory transactional broker, no Kafka required
npm run test --workspace nest-native-kafka-sample-05-transactions
npm run start --workspace nest-native-kafka-sample-05-transactions
```

## Against a real broker

```bash
KAFKA_BROKERS=localhost:9092 \
  npm run start --workspace nest-native-kafka-sample-05-transactions
```

Setting `KAFKA_BROKERS` switches to Confluent's `@confluentinc/kafka-javascript`
client. A real broker requires a transactional-capable cluster. Broker
credentials must never be committed to sample code, logs, or docs.
