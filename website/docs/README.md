# Documentation

Use the shortest path for the decision in front of you.

## Getting Started

- [Introduction](introduction.md): what the package does and does not own
- [Why Native](why-native.md): the correctness gaps it closes and why it is built on Confluent
- [Quick Start](quick-start.md): first module, producer, and consumer

## Core API

- [Module](module.md): `forRoot`, `forRootAsync`, and `forFeature`
- [Producer](producer.md): `KafkaProducerService` and `@InjectKafkaProducer()`
- [Consumers](consumers.md): `@KafkaConsumer` / `@KafkaHandler` and the enhancer pipeline
- [Parameter Decorators](parameter-decorators.md): `@KafkaMessage`, `@KafkaHeaders`, `@KafkaCtx`, `@KafkaBatch`

## Correctness

- [Error Mapping](error-mapping.md): exceptions to consumer behavior (`nestjs/nest#9679`)
- [Batch & Concurrency](batch-and-concurrency.md): per-topic concurrency (`#12703`) and rebalance-safe offsets (`#12355`)
- [Transactions](transactions.md): the transactional producer helper and `sendOffsets`
- [Graceful Shutdown](graceful-shutdown.md): stop, drain, disconnect

## Testing

- [Testing](testing.md): `KafkaTestModule`, the in-memory broker, and producer mocks

## Migration

- [Migration Guide](migration.md): porting off `@nestjs/microservices`'s Kafka transport

## Samples

- [Samples](samples/index.md): how to choose and run the sample applications
- [Sample Catalog](samples/catalog.md): feature-by-feature sample index

## Project Reference

- [API Reference](api-reference.md): the exported surface
- [Support Policy](support-policy.md): supported runtime and peer lines
- [Quality and CI](quality-and-ci.md): coverage, complexity, and release checks
- [Release Guide](release.md): package release workflow
- [Contributing](contributing.md): contribution rules and PR expectations
- [Roadmap](roadmap.md): current boundaries and future posture
