# Sample 00 — Showcase

The full integration baseline for `@nest-native/kafka`. It grows with each
milestone; today it demonstrates milestones 2 through 5.

What it shows:

- `KafkaModule.forRoot` wiring the driver and the producer service.
- Three feature modules (`orders`, `notifications`, `analytics`) each using
  `KafkaModule.forFeature` / a feature module to register a `@KafkaConsumer`.
- Producer + consumer wired together: the orders consumer publishes a derived
  notification that the notifications consumer handles.
- The full Nest enhancer pipeline on a handler: `@UseGuards`,
  `@UseInterceptors`, `@UsePipes`, `@UseFilters`.
- The parameter decorators `@KafkaMessage()`, `@KafkaHeaders()`, `@KafkaCtx()`
  (notifications consumer) and `@KafkaBatch()` (analytics consumer).
- Constructor dependency injection and a request-scoped provider
  (`OrderAuditService`) resolved per consumed message.
- A global module providing a shared singleton across features.
- **Batch consume + per-topic concurrency (milestone 5):** the `analytics`
  consumer aggregates order-revenue events one batch per partition
  (`batch: true`) with `concurrency: 2`, so tenant-keyed partitions are
  processed concurrently while staying ordered within a partition — the
  documented opt-out of the official transport's sequential per-topic processing
  (`nestjs/nest#12703`). `KafkaModule.forRoot({ maxInFlight })` caps in-flight
  work for backpressure.

Milestones still to land here: the transactional producer (6) and the testing
utilities + migration scenario (7).

## Run it

```bash
# in-memory loopback broker, no Kafka required
npm run test --workspace nest-native-kafka-showcase
npm run start --workspace nest-native-kafka-showcase
```

## Against a real broker

```bash
KAFKA_BROKERS=localhost:9092 \
  npm run start --workspace nest-native-kafka-showcase
```

`KAFKA_BROKERS` switches to Confluent's `@confluentinc/kafka-javascript` client.
Broker credentials must never be committed to sample code, logs, or docs.
