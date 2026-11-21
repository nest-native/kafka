# Sample 08 — The transactional outbox, over Kafka

Demonstrates `@nest-native/kafka/outbox`: the official
[`@nestjs/outbox`](https://www.npmjs.com/package/@nestjs/outbox) relay
publishing through `KafkaOutboxTransport`, on this package's producer, and a
plain `@KafkaHandler` on the other side that deduplicates with `OutboxInbox`.
Read [`website/docs/outbox.md`](../../website/docs/outbox.md) for the whole
contract.

What it shows:

- **One transaction, two writes.** `src/orders.service.ts` writes the order and
  its `orders.placed` message with `Outbox.add(tx, ...)` in one transaction. The
  message exists if and only if the order does; a transaction that throws takes
  both with it. `src/sample-outbox.store.ts` registers the outbox's in-memory
  store as the stand-in for a database; in production that is
  `PostgresOutboxStore` or `MySqlOutboxStore`, and `tx` is your ORM's.
- **The relay publishes after the commit.** `OutboxModule.forRoot` registers the
  transport by name. `KafkaModule` is global, so the transport class Nest
  instantiates gets the shared `KafkaProducerService`. A publish resolves on the
  broker's acknowledgement; a failure is retried with backoff, and a record
  Kafka can never accept (too large, an invalid topic) is dead-lettered at once.
- **The default wire, with a prefixed topic.** `src/topics.ts` configures the
  class with `KafkaOutboxTransport.with({ toRecord })`, returning only the
  topic: every other part of the record is the default, which is the record the
  official `ClientProxyTransport` sends through a `ClientKafka` — the message's
  key and headers, and the outbox envelope as the value.
- **The consumer reads the envelope and deduplicates.** `src/billing.consumer.ts`
  takes `{ id, topic, key, headers, createdAt, payload }` with `@KafkaMessage()`
  and charges through `OutboxInbox.process()`, so a redelivered record is
  skipped by its envelope id. Delivery is at-least-once; deduplication is the
  consumer's half of the contract.
- **`KafkaTestModule`.** `scripts/smoke.ts` runs the whole flow against the
  in-memory broker, driving the relay with `runOnce()`: the publish, the record
  on the wire, a redelivery that does not charge twice, and a rolled-back order
  that publishes nothing.

The properties an in-memory broker cannot show — a publish resolving on a real
acknowledgement, byte-level parity with a real `ClientKafka`, and the broker's
permanent refusals dead-lettered on the first attempt — are covered by the
package's `KAFKA_BROKERS`-gated integration suite.

## Run it

```bash
# in-memory, no Kafka required
npm run test --workspace nest-native-kafka-sample-08-outbox
npm run start --workspace nest-native-kafka-sample-08-outbox
```

## Against a real broker

```bash
KAFKA_BROKERS=localhost:9092 \
  npm run start --workspace nest-native-kafka-sample-08-outbox
```

Setting `KAFKA_BROKERS` switches to Confluent's
`@confluentinc/kafka-javascript` client. The sample produces to and consumes
`sample-08.orders.placed`. `@nestjs/outbox` is ESM-only and loads through
Node's `require(esm)`, so it needs Node.js `>=22.12`. Broker credentials must
never be committed to sample code, logs, or docs.
