# GUIDELINES_NEST_KAFKA.md

## Core Philosophy — This library MUST feel native in NestJS projects

Every decision must follow NestJS philosophy as `@nestjs/microservices` does,
while honestly addressing the correctness gaps that the official Kafka
transport accumulated. The bar is: feel like a first-class NestJS transport,
deliver on Confluent's officially supported client, never hide Kafka semantics.

### 1. Overall Architecture Assumptions (never break these)

- First-class NestJS integration, not a thin wrapper around the Confluent
  client.
- Decorator-first, OOP, heavy use of NestJS DI.
- Mirror the DX of `@nestjs/microservices` Kafka transport while explicitly
  solving its correctness issues (sequential per-topic processing, rebalance
  hangs, exception swallowing).
- Current stabilization support line:
  - Node.js `>=20`
  - NestJS `11.x`
  - `@confluentinc/kafka-javascript` `^1.9`
- Full integration with NestJS enhancer pipeline is NON-NEGOTIABLE:
  - `@UseGuards`, `@UseInterceptors`, `@UsePipes`, `@UseFilters` must work on
    handler methods.
  - Request-scoped providers, async providers, and `REQUEST` injection must
    work.
- Kafka has no HTTP coupling. The package is transport-only and does not
  impose HTTP adapter choices.
- Support both validation worlds for message payloads:
  - `class-validator` + DTOs via `ValidationPipe` (default for teams coming
    from `@nestjs/microservices`)
  - Zod (optional, for teams that prefer schema-derived types)

### 2. Public API Assumptions (this is what users will copy-paste)

- Module:
  - `KafkaModule.forRoot(options)`
  - `KafkaModule.forRootAsync(options)`
  - `KafkaModule.forFeature([HandlerClass])`
- Decorators:
  - `@KafkaConsumer('topic-or-pattern', options?)` — class-level
  - `@KafkaHandler('topic', options?)` — method-level
  - `@KafkaMessage()` — parameter, parsed payload
  - `@KafkaHeaders()` — parameter, headers
  - `@KafkaContext()` — parameter, raw transport context
- Producer:
  - `@InjectKafkaProducer()` for direct producer access
  - `KafkaProducerService` with `send`, `sendBatch`, `transactional`
- Testing:
  - `KafkaTestModule` with in-memory transport for unit tests
  - Driver-backed integration tests gated on a real Kafka in CI; skip
    locally if env missing
- A migration path from `@nestjs/microservices` Kafka transport must exist
  and stay current.

### 3. First-Version Scope Discipline

- v1 ships:
  - `KafkaModule.forRoot/forRootAsync/forFeature`
  - Consumer decorators with full enhancer support
  - Producer service (single + batch + transactional)
  - Header and context parameter decorators
  - Error-mapping helpers (NestJS exceptions → consumer behavior)
  - `KafkaTestModule` and producer mocks
  - One showcase sample + at least four focused samples
  - CI parity with the existing two nest-native packages
- v1 does NOT ship:
  - Confluent Schema Registry integration (follow-on package)
  - Exactly-once transactional helpers beyond what the client provides
  - A DLQ "framework" — provide primitives, document the pattern
  - AsyncAPI generation (belongs in `nest-asyncapi-native`)
  - Kafka Streams / KSQL / Connect

### 4. Sample Folder Rules

- `sample/00-showcase` demonstrates:
  - Producer + consumer wired together
  - Feature modules with handlers + services
  - Constructor DI, request-scoped providers
  - Guards, interceptors, pipes, filters on handler methods
  - Batch consumption + per-topic concurrency configured
  - Transactional producer
  - Graceful shutdown
  - Migration scenario from `@nestjs/microservices` Kafka
- Focused samples under `sample/01-*` ... `sample/09-*` isolate one topic with
  minimal noise (basics, enhancers, headers/context, Zod validation,
  class-validator validation, batch consume, error mapping + retries,
  transactions, microservice-app integration).
- Never simplify the showcase for brevity — richness proves the integration
  depth.

### 5. Implementation Rules

- The transport is a `CustomTransportStrategy` from `@nestjs/microservices`,
  backed by Confluent's client. Do not invent a new transport contract.
- Rebalance-safe consumption: in-flight messages must complete or be
  explicitly aborted; offsets commit only after successful handler return.
- Backpressure: cap in-flight messages per handler, configurable with a
  documented default.
- Per-topic concurrency: address `nestjs/nest#12703` explicitly with a
  documented default and an opt-out.
- Graceful shutdown order: stop accepting new claims → drain in-flight →
  disconnect.
- Header conventions stay neutral; do not standardize `traceId` /
  `correlationId` / `messageType` keys.
- A permanent kafkajs compatibility shim is NOT a feature. Migration
  ergonomics yes; permanent dual-runtime support no.
- Keep the package lean — minimal runtime dependencies. Published
  `"dependencies": {}`. Confluent client and Nest in `peerDependencies`.
- Never expose Confluent client internals to the user unless they opt in
  via advanced config.

### 6. Non-Negotiable Style & Patterns

- NestJS naming conventions (`@nestjs/common` style).
- Constructor injection.
- Always support global, module, and method-level enhancers.
- Tests must cover the enhancer pipeline, request scoping, rebalance
  behavior, backpressure, and graceful shutdown.
- Documentation and README follow Nest-style clarity without claiming
  official Nest or Confluent status.
- Preserve clear API tiers: onboarding focuses on `KafkaModule`, the core
  decorators, and the producer service. Advanced features stay in dedicated
  sections.

### 7. When In Doubt

- Ask: "Would this feel natural in `@nestjs/microservices`'s Kafka transport,
  while explicitly solving the gaps that one has?"
- If the answer is no, redesign.

### 8. Differentiation Strategy

- Built on `@confluentinc/kafka-javascript` (officially supported, actively
  maintained), not on `kafkajs`.
- Address known correctness issues (`#13223`, `#12703`, `#12355`, `#9679`)
  with explicit regression tests.
- Provide a documented migration path from `@nestjs/microservices` Kafka
  transport.
- Stay thin: users should feel they are using the Confluent client, just
  with NestJS DI and decorators around it.

### 9. Security Review Requirements (MANDATORY)

- Every PR includes an explicit security pass.
- Supply-chain checks are NON-NEGOTIABLE:
  - Every dependency addition/update reviewed for legitimacy.
  - `packages/kafka/package.json` must keep `"dependencies": {}`.
  - Runtime requirements in `peerDependencies`; build tools in
    `devDependencies`.
  - Inspect install/lifecycle scripts on every dep change.
  - Flag unpinned Git/URL dependencies.
- Application security checks:
  - Auth/authz risk in handlers and context wiring.
  - Input-validation gaps in deserialization (JSON, Avro, Protobuf).
  - SSL/SASL credential handling — never in samples, logs, or docs.
  - Topic ACL assumptions documented.
  - Secret leakage in payloads/headers shown in samples/tests/docs.

### 10. Release Version Synchronization (MANDATORY)

- Version drift between `packages/kafka` and `sample/*` is a release blocker.
- When bumping `packages/kafka/package.json`, update all
  `sample/*/package.json` entries for `"nest-kafka-native"` in the same
  change.
- Regenerate `package-lock.json`. Run `npm run release:check`. Run
  `npm run ci`.
- Post-publish: re-run full CI with samples pinned to the published version.

### 11. Cognitive Complexity Review

- When changes touch `packages/kafka/**/*.ts`, run `npm run complexity:check`
  and `npm run complexity:report`.
- CI enforces SonarJS cognitive-complexity threshold of `15` per package
  source function.
- Do not reduce complexity by weakening Nest-native architecture, public
  API clarity, rebalance safety, or test coverage.

### 12. Accumulated Project Decisions

(Empty at v0; grows as the project lands decisions worth preserving. Append
entries here when an architectural call repeats or is non-obvious. Each
entry should be one short paragraph with rationale.)
