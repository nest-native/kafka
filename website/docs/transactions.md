# Transactions

`KafkaProducerService.transactional(work)` runs `work` inside one Kafka
transaction. It commits when `work` resolves and aborts — delivering nothing —
when it throws, re-raising the original error. The package wraps only what the
Confluent client provides; it does not add exactly-once helpers beyond
Confluent's transactions.

## Enabling Transactions

Configure a `transactionalId` to make the shared producer transactional.
Confluent's client then also enables idempotence:

```ts
KafkaModule.forRoot({
  client: {brokers: ['localhost:9092']},
  producer: {transactionalId: 'orders-producer'}, // unique per producer instance
});
```

## Atomic Multi-Topic Write

Both writes land, or neither does:

```ts
await this.producer.transactional(async tx => {
  await tx.send({topic: 'orders.placed', messages: [{value: id}]});
  await tx.sendBatch({
    topicMessages: [{topic: 'orders.audit', messages: [{value: `placed ${id}`}]}],
  });
});
```

## Read-Process-Write With `sendOffsets`

For the consume-process-produce ("read-process-write") pattern, commit the
consumer's offset inside the same transaction with `sendOffsets`, so the produced
message and the consumed offset commit atomically — exactly-once across the
consume → produce step:

```ts
await this.producer.transactional(async tx => {
  await tx.send({topic: 'receipts.issued', messages: [{value: receipt}]});
  await tx.sendOffsets({
    consumer, // the live consumer object (see the migration note below)
    topics: [
      {
        topic: 'payments.captured',
        // commit "next offset to read" = consumed offset + 1
        partitions: [{partition, offset: String(Number(offset) + 1)}],
      },
    ],
  });
});
```

The `KafkaTransactionOffsets` / `KafkaTopicOffsets` / `KafkaPartitionOffset` types
model Confluent's shape.

:::note Migration note (kafkajs → Confluent)
`sendOffsets` takes the live `consumer` object in
`@confluentinc/kafka-javascript`, not the `consumerGroupId` string kafkajs used.
The `KafkaTransactionOffsets` type models the Confluent shape. See the
[Migration Guide](migration.md).
:::

## Abort Error Semantics

`transactional` commits on success and aborts on throw. If the abort itself fails
while unwinding a failed `work`, the original error still surfaces with the abort
failure attached as its `cause`, so neither error is lost.

## Sample

Sample `05-transactions` isolates the helper — atomic multi-topic write,
abort-on-throw, and `sendOffsets`. See the [Sample Catalog](samples/catalog.md).
