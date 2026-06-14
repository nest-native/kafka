# Roadmap

The v1 boundaries are deliberate. This page records what ships, what does not, and
the posture for the future.

## What v1 Ships

- `KafkaModule.forRoot` / `forRootAsync` / `forFeature`.
- Consumer decorators (`@KafkaConsumer`, `@KafkaHandler`) with the full enhancer
  pipeline and request scoping.
- Parameter decorators (`@KafkaMessage`, `@KafkaHeaders`, `@KafkaCtx`, `@KafkaBatch`).
- `KafkaProducerService` (single, batch, transactional) and `@InjectKafkaProducer()`.
- Error mapping from Nest exceptions to commit/retry behavior.
- Batch consumption, per-topic concurrency, backpressure, and graceful shutdown.
- `KafkaTestModule`, the in-memory broker, and producer mocks.
- One showcase sample plus focused samples, and CI parity with the family.
- A migration guide from `@nestjs/microservices`'s Kafka transport.

## What v1 Does Not Ship

Resisting scope creep into a "Kafka platform" is a stated goal:

- Confluent Schema Registry integration — a possible follow-on package
  (`nest-confluent-schema-registry`).
- Exactly-once helpers beyond what the Confluent client provides.
- A dead-letter-queue "framework" — the package provides primitives and documents
  the pattern. See [Error Mapping](error-mapping.md).
- AsyncAPI generation — belongs in `@nest-native/asyncapi`.
- Kafka Streams, ksqlDB, and Kafka Connect.

## Posture

- Track Confluent's `@confluentinc/kafka-javascript` major line and document the
  upgrade contract. See [Support Policy](support-policy.md).
- Keep the published package's runtime dependencies empty.
- Keep the migration path current as the official transport evolves. See the
  [Migration Guide](migration.md).

The bar never changes: feel like a first-class NestJS transport, deliver on
Confluent's officially supported client, and never hide Kafka semantics.
