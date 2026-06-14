# Samples

The sample tree follows the same shape as the main quality strategy:

- `00-showcase`: the full integration baseline; grows with each milestone.
- `01-*` onward: focused samples that isolate one topic each.

Current samples:

- `00-showcase` — producer + consumer wired together across three feature
  modules, the full enhancer pipeline, request-scoped DI, parameter decorators,
  a chained consumer, and a batch consumer with per-topic concurrency.
- `01-producer-basics` — `KafkaModule.forRoot`, the producer service, and a
  logging handler.
- `02-consumer-enhancers` — `@KafkaConsumer` / `@KafkaHandler` with guards,
  interceptors, pipes, and filters.
- `03-headers-context-errors` — the `@KafkaMessage` / `@KafkaHeaders` /
  `@KafkaCtx` parameter decorators, error mapping, and graceful shutdown.
- `04-batch-concurrency` — batch consumption (`@KafkaHandler({ batch: true })`,
  `@KafkaBatch()`), per-topic concurrency (`nestjs/nest#12703`), and
  rebalance-safe offset resolution (`nestjs/nest#12355`).
- `05-transactions` — the transactional producer helper
  (`KafkaProducerService.transactional`): atomic multi-topic writes, abort on
  throw, and the consume-process-produce `sendOffsets` pattern.
- `06-microservice-migration` — porting a `@nestjs/microservices` Kafka handler
  to `@nest-native/kafka` and testing it with `KafkaTestModule` and its in-memory
  broker (see [`docs/migration-from-nestjs-microservices.md`](../docs/migration-from-nestjs-microservices.md)).

## Commands

```bash
npm run ci:sample
npm run sample:focused
npm run test --workspace nest-native-kafka-showcase
npm run test --workspace nest-native-kafka-sample-02-consumer-enhancers
```

## Brokers

Samples run in memory by default so they need no Kafka broker and no native
`librdkafka` install. Set `KAFKA_BROKERS` (a comma-separated broker list) to run
a sample against a real cluster through Confluent's
`@confluentinc/kafka-javascript` client. Credentials must never be committed to
sample code, logs, or docs.
