# Sample 06 — Migrating from `@nestjs/microservices` Kafka

Demonstrates milestone 7: migrating a Kafka handler off `@nestjs/microservices`'s
official transport onto `@nest-native/kafka`, and testing the result with
`KafkaTestModule`.

What it shows:

- **Before / after.** `src/legacy-microservices.consumer.ts` holds the original
  `@Controller` + `@EventPattern` + `@Payload()`/`@Ctx()` handler (reference
  only). `src/orders.consumer.ts` is the ported `@KafkaConsumer` + `@KafkaHandler`
  + `@KafkaMessage()`/`@KafkaCtx()` version — a near-mechanical rename with the
  handler body unchanged.
- **`KafkaContext` parity.** `getTopic()` / `getPartition()` / `getMessage()`
  carry over unchanged, so handler bodies that read the context need no edits.
- **Producer migration.** `ClientKafka.emit(topic, payload)` becomes
  `KafkaProducerService.send({ topic, messages })`.
- **`KafkaTestModule`.** `scripts/smoke.ts` swaps `KafkaModule` for
  `KafkaTestModule`, which runs the whole transport against an in-memory broker.
  It asserts the ported consumer handles a produced order, inspects what the
  broker recorded with `broker.getSentTo(...)`, injects a message straight to
  the consumer with `broker.emit(...)`, and settles every in-flight handler
  pipeline with `broker.idle()` before asserting — no sleeps, no
  `@nestjs/testing`, no real Kafka, no native `librdkafka`.

The full field-by-field mapping lives in
[`docs/migration-from-nestjs-microservices.md`](../../docs/migration-from-nestjs-microservices.md).

## Run it

```bash
# in-memory KafkaTestModule, no Kafka required
npm run test --workspace nest-native-kafka-sample-06-microservice-migration
npm run start --workspace nest-native-kafka-sample-06-microservice-migration
```

## Against a real broker

```bash
KAFKA_BROKERS=localhost:9092 \
  npm run start --workspace nest-native-kafka-sample-06-microservice-migration
```

Setting `KAFKA_BROKERS` switches to Confluent's `@confluentinc/kafka-javascript`
client. Broker credentials must never be committed to sample code, logs, or docs.
