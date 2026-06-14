# Changelog

All notable user-facing changes to `@nest-native/kafka` are tracked here.

This project follows semantic versioning for the published package. Sample,
documentation, and CI-only changes may remain in `Unreleased` until the next
package release is useful for users.

## Unreleased

### Added

- `@KafkaConsumer(topic?, options?)` (class) and `@KafkaHandler(topic?, options?)`
  (method) decorators that register Kafka consumers and route messages to handler
  methods. Handlers run through the full Nest enhancer pipeline — `@UseGuards`,
  `@UseInterceptors`, `@UsePipes`, and `@UseFilters` all work, exactly as they do
  for `@nestjs/microservices` handlers — and request-scoped consumers resolve a
  fresh instance per consumed message.
- `KafkaContext`, the raw transport context exposed as the handler's second
  argument and through `ExecutionContext.switchToRpc().getContext()`.
- The driver gains `createConsumer(config?)`; the default Confluent driver
  forwards the resolved consumer group and advanced options to the client.
- Samples: `00-showcase` (producer + consumer across two feature modules with the
  full enhancer pipeline, request-scoped DI, and a chained consumer) and
  `02-consumer-enhancers` (a focused guard/interceptor/pipe/filter walkthrough).
- Batch consumption: `@KafkaHandler(topic?, { batch: true })` runs once per
  fetched topic-partition batch, with `@KafkaMessage()` resolving to the array of
  deserialized payloads and the new `@KafkaBatch()` decorator (and
  `KafkaBatchContext`) exposing the raw `KafkaConsumerBatch`.
- Per-topic concurrency (`nestjs/nest#12703`): a `concurrency` option on
  `KafkaModule.forRoot`, `@KafkaConsumer`, and `@KafkaHandler` sets the consumer's
  `partitionsConsumedConcurrently` (default `1`, strict per-partition ordering).
- Backpressure: a `maxInFlight` option (module / consumer / handler) caps how many
  messages or batches a consumer processes at once (default uncapped).
- Rebalance-safe batch offsets (`nestjs/nest#12355`): batch consumers resolve each
  message's offset as it is processed instead of relying on the client's
  all-or-nothing batch auto-resolve.
- Sample `04-batch-concurrency` demonstrating batch consume, per-topic
  concurrency, and rebalance-safe offset resolution; the showcase gains an
  `analytics` batch consumer.
- Transactional producer helper: `KafkaTransaction` gains `sendOffsets` for the
  consume-process-produce ("read-process-write") pattern, with the
  `KafkaTransactionOffsets` / `KafkaTopicOffsets` / `KafkaPartitionOffset` types
  modelling Confluent's shape (the live `consumer` object, not kafkajs's
  `consumerGroupId` string). `KafkaProducerService.transactional` commits on
  success and aborts on throw, and now preserves the original error (attaching a
  failed abort as its `cause`) so neither error is lost. `KafkaProducerConfig`
  documents `transactionalId`. Sample `05-transactions` isolates the helper
  (atomic multi-topic write, abort-on-throw, and `sendOffsets`); the showcase's
  `OrdersService.placeOrder` now publishes transactionally.

## 0.0.0 - 2026-06-13

### Added

- Initial repository scaffold (`v0.0.1-scaffold` milestone).
- npm workspace skeleton for `@nest-native/kafka` with `node:test` + `c8`
  coverage (enforced at 100%), ESLint + SonarJS cognitive-complexity gate
  (threshold `15`), `tsc`-only build, package tarball validation, README link
  validation, and a high-severity supply-chain audit.
- `KafkaModule` shell exposing `KafkaModule.forRoot()`,
  `KafkaModule.forRootAsync()`, and `KafkaModule.forFeature()`. `forRoot` and
  `forRootAsync` return a global `DynamicModule` that provides the resolved
  module options; `forFeature` returns a non-global module that registers and
  exports the supplied handler classes. The consumer decorators
  (`@KafkaConsumer`, `@KafkaHandler`), parameter decorators (`@KafkaMessage`,
  `@KafkaHeaders`, `@KafkaContext`), and `KafkaProducerService` are
  intentionally not yet implemented.
- CI for build, typecheck, and coverage on Node.js 20 and 22, sticky PR
  comments for coverage, test performance, and cognitive complexity, plus
  release and supply-chain checks.

The published package keeps `"dependencies": {}`. The Confluent client
(`@confluentinc/kafka-javascript`) and the NestJS packages are declared as
`peerDependencies`. The native Confluent client is a peer-only dependency and is
intentionally not installed at this milestone.
