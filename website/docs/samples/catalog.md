# Sample Catalog

A feature-by-feature index of the samples. Browse the source under
[`sample/`](https://github.com/nest-native/kafka/tree/main/sample) in the
repository.

## `00-showcase`

The full integration baseline. Producer and consumers wired together across
feature modules, the full enhancer pipeline, request-scoped DI, parameter
decorators, a chained consumer, a transactional producer, and a batch consumer
with per-topic concurrency. Never simplified for brevity — richness proves the
integration depth.

Related docs: [Consumers](../consumers.md), [Producer](../producer.md),
[Transactions](../transactions.md).

## `01-producer-basics`

The smallest runnable app: `KafkaModule.forRoot`, the `KafkaProducerService`, and
a single handler that logs every message it receives.

Related docs: [Quick Start](../quick-start.md), [Producer](../producer.md).

## `02-consumer-enhancers`

`@KafkaConsumer` / `@KafkaHandler` running through guards, interceptors, pipes, and
filters — a focused walkthrough of the enhancer pipeline on the Kafka transport.

Related docs: [Consumers](../consumers.md).

## `03-headers-context-errors`

The `@KafkaMessage` / `@KafkaHeaders` / `@KafkaCtx` parameter decorators, error
mapping, and graceful shutdown.

Related docs: [Parameter Decorators](../parameter-decorators.md),
[Error Mapping](../error-mapping.md), [Graceful Shutdown](../graceful-shutdown.md).

## `04-batch-concurrency`

Batch consumption (`@KafkaHandler({batch: true})`, `@KafkaBatch()`), per-topic
concurrency ([`nestjs/nest#12703`](https://github.com/nestjs/nest/issues/12703)),
and rebalance-safe offset resolution
([`nestjs/nest#12355`](https://github.com/nestjs/nest/issues/12355)).

Related docs: [Batch & Concurrency](../batch-and-concurrency.md).

## `05-transactions`

The transactional producer helper (`KafkaProducerService.transactional`): atomic
multi-topic writes, abort on throw, and the consume-process-produce `sendOffsets`
pattern.

Related docs: [Transactions](../transactions.md).

## `06-microservice-migration`

Porting a `@nestjs/microservices` Kafka handler to `@nest-native/kafka` and
testing it with `KafkaTestModule` and its in-memory broker.

Related docs: [Migration Guide](../migration.md), [Testing](../testing.md).
