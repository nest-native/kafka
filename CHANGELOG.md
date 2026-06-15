# Changelog

All notable user-facing changes to `@nest-native/kafka` are tracked here.

This project follows semantic versioning for the published package. Sample,
documentation, and CI-only changes may remain in `Unreleased` until the next
package release is useful for users.

## Unreleased

## 0.1.1 - 2026-06-15

A documentation-truth and CI-hardening release. No public API changes.

### Added

- A real-broker CI integration test (`packages/kafka/test/kafka.integration.spec.ts`)
  and a `test:integration` npm script, gated on `KAFKA_BROKERS`. A dedicated
  `integration` CI job stands up a single-node KRaft Kafka (`apache/kafka`) and
  runs a real produce → consume round-trip, a transactional commit via
  `KafkaProducerService.transactional`, and per-topic concurrency with durable
  offset commits against the live broker. The suite is skipped when
  `KAFKA_BROKERS` is unset, so it never affects the 100% coverage gate, and it
  installs the optional `@confluentinc/kafka-javascript` peer on-demand (the
  published package still keeps `"dependencies": {}`).

### Docs

- Dropped the stale "scaffold / under construction" framing from the root
  `README.md` (removed the scaffold status badge, the warning block, and the
  "consumer decorators and the producer service are not implemented yet" line;
  reframed the milestone roadmap as "what shipped in v0.1"; added docs and
  downloads badges) and from `CONTRIBUTING.md`. Documented that
  `@confluentinc/kafka-javascript` stays an optional peer but is required for
  real (non-test) usage. Reconciled `website/docs/quality-and-ci.md` so the
  real-broker integration claim is accurate.

## 0.1.0 - 2026-06-14

First published release. The full v1 surface — the module, the producer service,
consumer decorators with the complete Nest enhancer pipeline, the parameter
decorators, error mapping, batch consumption with per-topic concurrency, the
transactional producer, `KafkaTestModule`, the migration guide, and the
documentation site — is in place. The published package keeps `"dependencies": {}`.

### Added

- A Docusaurus documentation site under `website/` covering getting started, the
  core API (module, producer, consumers, parameter decorators), the correctness
  guarantees (error mapping `nestjs/nest#9679`, batch + per-topic concurrency
  `nestjs/nest#12703`, rebalance-safe offsets `nestjs/nest#12355`, transactions,
  graceful shutdown), testing, the migration guide, the sample catalog, and the
  project reference. CI gains a `docs-site` build job and a `deploy-docs` workflow
  that publishes the site to GitHub Pages; `npm run ci` now runs `ci:docs` and a
  docs supply-chain audit alongside the package checks.
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
- Testing utilities, re-exported from the package root: `KafkaTestModule`
  (`forRoot` / `forRootAsync`) runs the whole transport — producer service, the
  `@KafkaConsumer` enhancer pipeline, batch consumption, transactions, graceful
  shutdown — against an in-memory `InMemoryKafkaBroker`, with no real broker and
  no native `librdkafka`. The broker is injectable via the `KAFKA_TEST_BROKER`
  token or `@InjectKafkaTestBroker()` and exposes `emit()` to inject consumed
  messages and `getSent()` / `getSentTo()` to assert on produced ones.
  `createMockKafkaProducer()` / `createMockTransaction()` provide recording
  producer mocks for unit-testing services that inject the producer without a Nest
  module.
- A migration guide from `@nestjs/microservices`'s Kafka transport
  (`docs/migration-from-nestjs-microservices.md`): the decorator/parameter/
  producer mapping plus the behavioural deltas (explicit serialization, exception
  mapping `nestjs/nest#9679`, per-topic concurrency `nestjs/nest#12703`,
  rebalance-safe batch offsets `nestjs/nest#12355`, the Confluent `sendOffsets`
  shape, and backpressure). Sample `06-microservice-migration` ports a handler
  end-to-end and validates it with `KafkaTestModule`.

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
