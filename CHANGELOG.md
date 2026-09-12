# Changelog

All notable user-facing changes to `@nest-native/kafka` are tracked here.

This project follows semantic versioning for the published package. Sample,
documentation, and CI-only changes may remain in `Unreleased` until the next
package release is useful for users.

## Unreleased

### Changed

- **Both ends of the NestJS peer range are now CI legs.** The single
  `nestjs-latest-major` job that installed `^12` is replaced by a
  `nestjs-compat` matrix: an `11 floor` leg pinned exactly to `11.0.0` (the
  oldest graph the published `^11.0.0 || ^12.0.0` range can produce, with the
  reason next to the pin) and a `12` leg on `^12.0.0`. Each leg greps its
  install log for `ERESOLVE` (npm overrides a peer conflict it can override
  with a warning and exit 0) and runs `scripts/check-nestjs-resolution.mjs`,
  which proves the exact version from inside every workspace and re-checks
  every `@nestjs/*` peer range in the tree. The same script runs against the
  lockfile in `release:check`. No published range changed.

## 0.5.1

### Changed

- **NestJS 12 is supported.** The `@nestjs/common`, `@nestjs/core`, and
  `@nestjs/microservices` peer ranges widen from `^11.0.0` to
  `^11.0.0 || ^12.0.0`. NestJS 12 is ESM-only with an exports map, under which
  `@nestjs/common/interfaces` — a directory, and this package's only directory
  import into `@nestjs/*` — no longer resolves. That one import was the whole
  failure on 12: four sites, a `TS2307` on the build, and 10 of the 21 spec
  files unable to load (111 of 121 tests). Every other deep import names a
  file and still resolves. `Controller` is now a local
  alias (plain `object`, exactly what `@nestjs/common@12` declares), so the
  published `.d.ts` files no longer reference that path either. A new test
  scans every `@nestjs/*` deep import and requires it to name a file, and a
  dedicated CI leg installs 12 on top of the 11.x lockfile in every workspace
  and runs the unit suite, the build, and the sample matrix, so both ends of
  the range are tested. The devDependencies stay on 11.x. The 12 end of the
  range needs Node.js `>=22.12`, where `require(esm)` is no longer behind a
  flag; `engines` stays `>=22` because the 11 end does not need more. NestJS
  12 also reordered lifecycle hooks across providers; nothing here depends on
  a cross-provider hook order.

### Added

- **Request-reply, as an opt-in migration bridge** (ADR 0001,
  `docs/adr/0001-request-reply.md`). `@KafkaHandler` stays fire-and-forget and
  Kafka stays the event log it is; what this adds is the one piece a
  `@MessagePattern` / `ClientKafka.send()` application cannot safely hand-roll —
  making a reply reach the *instance* that asked, across rebalances, restarts,
  and N replicas behind a load balancer. Until then the migration guide told
  users to build that themselves, which was bad advice at scale.

  - Replying side: `@KafkaHandler(topic, { reply: true })` answers with the
    handler's post-enhancer return value, addressed by the request's own
    headers — so a replier needs no module configuration at all. `reply` with
    `batch`, and two repliers on one topic, are refused at bootstrap.
  - Requesting side: `KafkaRequestReplyService.request<T>()` with a 30s default
    timeout, per-call overrides, and `AbortSignal` support, plus the exported
    `KafkaReplyTimeoutError`, `KafkaReplyRemoteError`, `KafkaReplyAbortedError`,
    and `KafkaReplyDeliveryError`.
  - Routing: one shared reply topic, one single-member ephemeral consumer group
    per instance, correlation-id filtering. The correctness argument is an
    absence of machinery — a group of one has nothing to rebalance — and the
    cost is N-times reply fan-out, documented with arithmetic.
  - Interop: the default header keys are `@nestjs/microservices`' own, so a
    partially migrated fleet works in both directions with no configuration on
    either side. Pinned by contract tests that run a real `ServerKafka` and a
    real `ClientKafka` against a real broker.
  - The feature is inert at runtime until `requestReply` is configured, and the
    published package still ships `"dependencies": {}` — correlation ids come
    from `node:crypto`. `kafkajs` enters **dev**-only scope, for the interop
    contract tests.

  See `website/docs/request-reply.md`, including the section on when
  request-reply over Kafka is the wrong tool, and `sample/07-request-reply`.

### Fixed

- **`fromBeginning` is no longer passed to `subscribe()`.** `kafkajs` accepts it
  there; Confluent's compatibility layer rejects it outright with
  `ERR__INVALID_ARG` and reads it only at consumer creation. The reply consumer
  was the only caller, and the in-memory broker ignores subscribe options, so
  the real-broker suite was the first thing that could see it. The field on
  `KafkaSubscription` is now documented as unusable and deprecated.

## 0.5.0

### Added

- **Opt-in request-reply** ([ADR 0001](docs/adr/0001-request-reply.md)). Every
  `@MessagePattern` user of `@nestjs/microservices` was previously unable to
  migrate to this package at all: `ClientKafka.send()` is request-reply, and the
  migration guide's answer was to hand-roll correlation. Now
  `KafkaRequestReplyService.request<T>()` issues a request with a default 30s
  timeout and `AbortSignal` support, and `@KafkaHandler(topic, { reply: true })`
  answers with the handler's post-enhancer return value.

  `@KafkaHandler` stays fire-and-forget by default and the client side is inert
  until `requestReply` is configured, so nothing changes for existing users.
  Kafka-as-RPC remains an anti-pattern this package does not argue away — the
  feature is a bounded migration bridge, and the docs say when not to reach for
  it.

  **Routing:** one shared reply topic consumed by a unique single-member
  ephemeral consumer group per instance, filtered by correlation id. The
  official transport's partition-per-instance design needs a custom partition
  assigner `librdkafka` cannot express, couples partition count to replica
  count, and loses in-flight replies on rebalance. A group of one has nothing to
  rebalance. The cost is N× reply fan-out, which is linear, documented with
  arithmetic, and the reason the strategy seam exists.

  **Interop is proven byte-level against the real `@nestjs/microservices`**, in
  both directions, in CI: our `request()` answered by a `ServerKafka`
  `@MessagePattern` handler, and a real `ClientKafka.send()` answered by a
  `reply: true` handler. Header keys default to the official `kafka_*` names and
  are configurable.

  **The reply path is at-most-once and a timeout means "unknown outcome"** —
  never "it did not happen". There are no transport-level retries; re-sending is
  the caller's decision. The `errorMapper` contract is unchanged: `'commit'`
  sends an error reply, `'retry'` sends none and lets the broker redeliver.

### Fixed

- **The in-memory broker invented partitions from the array index** in
  per-message delivery, ignoring `message.partition` — while batch delivery read
  it correctly. Any test asserting per-message partitions was being told a
  number the producer never chose.

## 0.4.1

### Fixed

- **Undotted `librdkafka` properties were still unreachable.** 0.4.0 routed
  configuration by looking for a dot, on the reasoning that every `librdkafka`
  property is dotted. It is not: the client's own config types declare 32
  undotted properties, so `debug`, `log_level`, `retries`, `partitioner`,
  `enabled_events`, the `*_cb` callbacks and the flat `ssl_*` keys still landed
  inside `kafkaJS` and still failed at `connect()` with "The '<name>' property
  is not supported" — the exact failure 0.4.0 set out to remove. Notably
  `debug` is the property most used to diagnose the connection behaviour this
  package leans on.

  Those names are now routed explicitly, and a new test reads the *installed*
  client's type definitions and fails if the list falls behind a client
  release, so the table cannot rot silently. `acks` is deliberately excluded —
  it is the one undotted name the KafkaJS layer also accepts, and routing it
  would break configuration that works today.

- **The drift guard now actually runs in CI.** It skips wherever the optional
  Confluent peer is absent, which is every job except the integration one — so
  as first written it was a gate in name only. The integration job, the one
  place the peer is installed, now runs it as its own step
  (`npm run test:peer-drift`).

- **Docs no longer overclaim.** The resilience page said any `librdkafka`
  property could be set, which was untrue for exactly the properties above. It
  now describes what is actually routed and how the list is kept honest.

## 0.4.0

### Added

- **`librdkafka` tunables now actually reach the client.** Configuration passed
  to `client`, `producer`, and `consumer` was forwarded wholesale under the
  Confluent client's `kafkaJS` key, where the compatibility layer rejects raw
  dotted properties with "The '<name>' property is not supported." The escape
  hatch the config types advertised was therefore unusable: `reconnect.backoff.ms`,
  `socket.keepalive.enable`, `metadata.max.age.ms` and every other `librdkafka`
  property were unreachable. Options are now split on the dot — dotted properties
  go to the top level the client reads them from, KafkaJS-style options stay
  under `kafkaJS` — so both families can be mixed in one object.

- **Broker-restart recovery is proven, not asserted.** The real-broker
  integration suite restarts the broker container out from under a running
  application, severing every connection the client holds, then requires the same
  handler to receive a message published afterwards. Nothing in between restarts
  the application, re-creates the producer, or re-subscribes the consumer, so
  recovery has to come from the client itself. The case is gated on
  `KAFKA_RESTART_CONTAINER` naming a container the suite may restart, so pointing
  `KAFKA_BROKERS` at a shared or managed cluster never restarts anything.

- **Docs: a Resilience and Reconnection page** stating what recovers
  automatically, what the integration test proves, and — deliberately — what
  recovery does *not* promise: produce calls during an outage can still fail,
  redelivery remains at-least-once, and a rebalance can move partitions.

### Changed

- Docs: the version literals in prose told the wrong story — both READMEs
  announced `Status: 0.1.1`, `CONTRIBUTING.md` said the package was published at
  `0.1.x`, and the release guide still described the `0.1.0` release, while npm
  had `0.3.0`. All of them now state the published version, and the release guide
  summarizes what each release since `0.1.0` added. A new release gate,
  `npm run release:check:readme-version` (`scripts/check-readme-version.mjs`,
  wired into `release:check`), fails on any future drift: it reads the version
  from every non-private `packages/*/package.json`, checks the README `Status:`
  literals and the `CONTRIBUTING.md` release line against it, and rejects
  hardcoded `img.shields.io/badge/version-…` badges so version badges stay
  dynamic.

- Fixed a misleading error when the optional `@confluentinc/kafka-javascript`
  peer fails to load. It is a **native addon**, so a binary built for another
  Node.js major throws `ERR_DLOPEN_FAILED` even though the package is installed
  — and the driver reported every load failure as "is not installed", sending
  people to hunt a dependency problem they did not have. The two cases now read
  differently: a genuine `MODULE_NOT_FOUND` still says "not installed", while
  anything else says "installed but failed to load", quotes the underlying
  error (newlines collapsed, so the `NODE_MODULE_VERSION` diagnosis survives),
  and suggests rebuilding. The original error is still attached as `cause` in
  both cases.

- Tests: closed a mutation-testing gap in `defaultKafkaErrorMapper` — added a
  sub-4xx `HttpException` case proving only the 4xx band commits (everything
  else, including sub-400, is retried). Documented the one genuine equivalent
  mutant in `deserializeKafkaValue` (the `value === null` tombstone fast-path
  is redundant because `JSON.parse(null)` is `null`) with an inline
  Stryker-disable. No behavior or API change.
- Local full-mode verification and mutation testing (repo tooling; nothing
  ships in the package): `compose.yaml` + `npm run infra:up`/`infra:down`
  start a disposable single-node KRaft Kafka on `127.0.0.1:19094`,
  `npm run test:full` runs the gated real-broker integration suite against
  it, and Stryker mutation testing is available via `npm run test:mutation`
  (incremental) / `test:mutation:full` with `STRYKER_MUTATE` scoping and
  `STRYKER_WITH_INFRA=1` for broker-inclusive runs. All of it is opt-in and
  local-only — CI is unchanged and Docker-free. See the new "Local Full-Mode
  Verification" section in GUIDELINES_NEST_KAFKA.md.

## 0.3.0 - 2026-07-01

### Added

- **`InMemoryKafkaBroker.idle()`** — an awaitable settle point for tests. It
  resolves once every in-flight `@KafkaHandler` pipeline has settled, looping
  until the broker is quiet so cascaded dispatches are included (a handler
  fire-and-forgets an audit/DLQ produce, another consumer handles it, and so
  on down the chain). Tests using `KafkaTestModule` can replace fixed sleeps
  after `broker.emit(...)` / a producer send with `await broker.idle()`: it is
  exact, it resolves even when handlers throw (error mapping has already
  decided commit-vs-retry), and it does not stop consumption. Work a handler
  schedules outside the dispatch chain (a bare `setTimeout`) remains invisible
  to the broker. The production path is untouched — tracking lives entirely in
  the in-memory broker.

## 0.2.0 - 2026-06-23

### Changed

- **BREAKING (testing entrypoint).** The testing utilities — `KafkaTestModule`,
  `InMemoryKafkaBroker`, `createMockKafkaProducer`/`createMockTransaction`,
  `InjectKafkaTestBroker`, and the `KAFKA_TEST_BROKER` token — are no longer
  re-exported from the package root. Import them from the new
  **`@nest-native/kafka/testing`** entrypoint instead. This keeps test
  scaffolding (the in-memory broker, mock producer) out of consumers' production
  import surface and bundles. Runtime exports (`KafkaModule`,
  `KafkaProducerService`, decorators, driver, tokens) are unchanged.

### Added

- A `./testing` export subpath (`@nest-native/kafka/testing`) for the testing
  utilities.

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
