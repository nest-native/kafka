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
  - Node.js `>=22` (`>=22.12` on the NestJS 12 end: 12 is ESM-only and
    `require(esm)` is behind a flag before 22.12.0; `engines` stays `>=22`
    for the 11 end)
  - NestJS `^11.0.0 || ^12.0.0`
  - `@confluentinc/kafka-javascript` `^1.9`
- **Peer majors are widened, never swapped.** When a peer ships a new major,
  the published `peerDependencies` range widens to include it, the
  devDependency (and therefore the lockfile every default CI job installs)
  stays on the older major so the default suite keeps testing that end, and a
  dedicated CI leg installs the newer major with `--no-save` and runs the
  suite and the samples. Both ends of the range are then tested claims. A
  dependabot PR that moves the devDependency to the new major is not how a
  major gets adopted — see §12.
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
  - **Opt-in request-reply** (ADR 0001), added in a 0.x minor and bounded to:
    `@KafkaHandler(topic, { reply: true })` answering with the handler's
    post-enhancer value at whatever address the request's headers name;
    `KafkaRequestReplyService.request()` with a default timeout and
    `AbortSignal`; `requestReply` module options; **one** routing strategy —
    a shared reply topic consumed by a unique single-member ephemeral group per
    instance, with correlation-id filtering; a header contract defaulting to
    `@nestjs/microservices`' five key names and configurable; and the four
    exported error classes. `@KafkaHandler` stays fire-and-forget, and the
    feature is inert at runtime until it is configured.
- v1 does NOT ship:
  - Confluent Schema Registry integration (follow-on package)
  - Exactly-once transactional helpers beyond what the client provides
  - A DLQ "framework" — provide primitives, document the pattern
  - AsyncAPI generation (belongs in `@nest-native/asyncapi`)
  - Kafka Streams / KSQL / Connect
  - Request-reply beyond the bounded surface above. Each of these was
    considered and declined in ADR 0001 §7; adding one is a decision, not a
    detail:
    - **Streaming or multiple replies** (the official observable protocol). The
      single-promise contract is the honest shape for an at-most-once reply, and
      it is structurally excluded by the observable normalization rather than
      merely unimplemented.
    - **Scatter-gather / broadcast requests.** First-reply-wins is the
      documented behaviour if several groups answer; aggregation is an
      application concern.
    - **Requester-side automatic retries.** A timeout means the outcome is
      unknown, so a transport-level retry on top of it manufactures duplicates.
      Re-sending is the caller's decision.
    - **A second routing strategy** (per-instance reply topics). Additive later
      by design — the reply address travels in the request's headers — but
      shipping two strategies at once means shipping two half-tested ones.
    - **A `ClientProxy`-compatible adapter.** Migration ergonomics, not
      permanent dual-runtime support; the same reasoning that rules out a
      kafkajs shim.
    - Also out: admin/topic-creation tooling (the reply topic is provisioned
      infrastructure like every other topic), reply-topic allowlists on the
      replier (broker ACLs are the enforcement layer), and metrics hooks for
      dropped or late replies (debug logs are the answer, as everywhere else).

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
  transactions, microservice-app integration, request-reply).
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
  `correlationId` / `messageType` keys. **One bounded exception:** the opt-in
  request-reply feature cannot be header-neutral — a reply address and a
  correlation id have to live somewhere with agreed names — so its keys are part
  of *its* contract and are configurable (`requestReply.headers`), defaulting to
  `@nestjs/microservices`' own names for interop. The exception is scoped to
  request-reply; general messaging stays neutral, and `KafkaContext.getHeaders()`
  still returns the raw map untouched. See §12 and ADR 0001 §4.
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
- **Audit scope.** The `security:audit` release gate audits the *published*
  surface — `npm audit --omit=dev --audit-level=high`. Since the package
  publishes `"dependencies": {}`, this is exactly what consumers install.
  Advisories confined to dev/peer/build tooling or the docs `website/` are
  tracked and patched via Dependabot but do not block releases — they cannot
  reach consumers. Patch them in their own PRs.
- **The docs audit reports, it does not gate.** `security:audit` hard-fails only
  on the *published* surface; `security:audit:docs` still runs and prints, but
  cannot fail the build. This makes the gate match the rule above — website
  advisories cannot reach consumers, so they must not block every PR in the
  repo. Precedent: `@nest-native/cache` and `@nest-native/trpc` were already
  package-only. Trigger: `image-size` (GHSA-w3rx-r6r6-pgpr,
  GHSA-5p2g-fcmc-qvqq) has NO patched version — 2.0.2 is both the latest
  release and vulnerable — and arrives through `@docusaurus/mdx-loader`, so the
  gate was unfixable by any dependency change. Dependabot still tracks the
  website tree; fix docs advisories when a fix exists.

- **Strictness scope.** The non-negotiables (100% coverage, cognitive-complexity
  ≤ 15, zero published runtime deps, isolated major-version review) govern the
  *core* published package (`packages/kafka`). Non-core code — `sample/*`, the
  `website/`, and dev tooling — uses lighter rules: their dependency updates
  (including majors) may merge on green CI without the core's major-isolation
  ceremony.

### 10. Release Version Synchronization (MANDATORY)

- Version drift between `packages/kafka` and `sample/*` is a release blocker.
- When bumping `packages/kafka/package.json`, update all
  `sample/*/package.json` entries for `"@nest-native/kafka"` in the same
  change.
- Regenerate `package-lock.json`. Run `npm run release:check`. Run
  `npm run ci`.
- Post-publish: re-run full CI with samples pinned to the published version.
- **Prose version literals are release-blocking too.** The bolded `Status:` line
  in `README.md` and `packages/kafka/README.md`, the published release line in
  `CONTRIBUTING.md`, badges, and any compatibility table must state the version
  that is actually published. A stale literal is a documentation lie, not a
  cosmetic nit: it is the first thing a user reads and it silently contradicts
  npm. `release:check:readme-version`
  (`scripts/check-readme-version.mjs`) enforces this and fails the gate on drift.
- **Prefer dynamic badges over hardcoded ones.** Version and status badges must be
  generated (`img.shields.io/npm/v/@nest-native/kafka.svg`), never hand-written —
  a hardcoded badge is drift waiting to happen. `release:check:readme-version`
  rejects `img.shields.io/badge/version-…` and `img.shields.io/badge/status-…`
  literals outright.
- **Version-sync checks iterate, they do not hardcode.** Any check in `scripts/`
  that reasons about the published version must enumerate every non-private
  `packages/*/package.json` and read the version from there, rather than hardcoding
  a package name or a version string. The repo ships one package today; a check
  written to that assumption goes quietly blind the day a second one lands.

### 11. Cognitive Complexity Review

- When changes touch `packages/kafka/**/*.ts`, run `npm run complexity:check`
  and `npm run complexity:report`.
- CI enforces SonarJS cognitive-complexity threshold of `15` per package
  source function.
- Do not reduce complexity by weakening Nest-native architecture, public
  API clarity, rebalance safety, or test coverage.

### 12. Accumulated Project Decisions

(Grows as the project lands decisions worth preserving. Append entries here when
an architectural call repeats or is non-obvious. Each entry should be one short
paragraph with rationale.)

**Request-reply is an opt-in bridge, and the event log stays the default**
(ADR 0001, `docs/adr/0001-request-reply.md`). Kafka-as-RPC is an anti-pattern
this package does not argue away — but a documented migration path from
`@nestjs/microservices` is a headline promise, and without request-reply that
promise excluded every `@MessagePattern` user. So it ships, bounded: `@KafkaHandler`
stays fire-and-forget unless a handler declares `reply: true`, and the client side
does nothing at all unless `requestReply` is configured. Two decisions inside it
are load-bearing and must not be undone casually. **Reply routing** is one shared
reply topic consumed by a unique single-member ephemeral group per instance
(`<replyTopic>-<uuid>`), filtering by correlation id — the official transport's
partition-per-instance design depends on a custom partition assigner that
librdkafka cannot express, couples partition count to replica count, and loses
in-flight replies on rebalance. The chosen strategy's correctness argument is the
*absence* of machinery: a group of one has nothing to rebalance. Its cost is N×
reply fan-out, which is linear and documented; the strategy seam is the planned
exit if that ever bites. **The reply path is at-most-once and a timeout means
"unknown outcome"** — never "it did not happen" — and the docs, the error
messages, and the absence of transport-level retries all have to keep saying so.
The `errorMapper` contract is unchanged by the feature: `'commit'` means done, so
an error reply is sent; `'retry'` means not done yet, so no reply exists to send
and the broker redelivers.

**The in-memory broker cannot validate anything about the driver surface, so any
new use of it needs a real-broker case in the same PR.** `InMemoryKafkaBroker`
accepts every consumer config and every subscribe option without looking at
them, which makes it silently agreeable about arguments the Confluent client
rejects outright. Request-reply shipped with `fromBeginning` passed to
`subscribe()` — valid in `kafkajs`, an `ERR__INVALID_ARG` on this package's
client, and the difference between "100% covered" and "the feature cannot start"
(it belongs in the consumer config instead). Its readiness probe had the matching
disease one level up: a single sentinel produced before the group's first
assignment is lost to the very latest-offset race the probe exists to detect, and
an in-memory subscription that is live the instant it is registered can never
show that. The rule that follows: when a change starts calling a driver method
with a new argument, or depends on real group-assignment timing, the
`KAFKA_BROKERS`-gated suite gets a case for it in the same PR — unit coverage of
that code proves only that we called ourselves consistently.

**NestJS majors are adopted by widening the peer range, and the `@nestjs/*`
devDependencies stay on the older major.** NestJS 12 (2026-08) was added as
`^11.0.0 || ^12.0.0` on `@nestjs/common`, `@nestjs/core`, and
`@nestjs/microservices`, with the devDependencies and the lockfile left on 11.x
and a `nestjs-latest-major` CI leg that installs 12 on top of that lockfile
(`npm install --no-save --workspaces --include-workspace-root`, because the
samples pin `@nestjs/*` exactly and a root-only install leaves them a nested
11) and runs the unit suite, the package build, and the sample matrix. The
leg asserts from inside every workspace that `@nestjs/core` resolves to 12
before it runs anything, so a hoisting accident cannot turn it into a second
11 leg. Dependabot cannot deliver a NestJS major: the `@nestjs/*` packages peer
on each other, so one-package-per-PR bumps fail `npm ci` with ERESOLVE before
a single test runs (NestJS 12 opened fifteen such PRs across the org). The
remedy is a dependabot group that carries majors, so the next major arrives
as one PR whose result carries information; PR #58 adds that group to
`.github/dependabot.yml` in its own change, and every `@nestjs/*` package
the repo declares must be listed in it. Even that PR is evidence for the
peer-widening recipe above, not a replacement for it.

**NestJS 12 is ESM-only: never import a directory index from `@nestjs/*`.**
`@nestjs/common` and `@nestjs/core` 12 ship an exports map of
`{".", "./internal", "./*.js", "./*": "./*.js"}`. Every deep import of a *file*
this package makes into `@nestjs/core` (`injector/constants`,
`helpers/execution-context-host`, the guard/pipe/interceptor consumers and
context creators, ...) still resolves under it; a deep import of a *directory*
does not, because there is no `<dir>.js` and ESM never completes a directory
to its `index`. `@nestjs/common/interfaces` was the one such import in the
package, and on 12 it was the whole failure: four sites, a `TS2307` on the
build, and 10 of the 21 spec files unable to load (111 of 121 tests).
`Controller` is now a local alias in
`kafka-params.resolver.ts` (plain `object`, exactly what `@nestjs/common@12`
declares; the type is not re-exported from the `@nestjs/common` root). The
rule is enforced by `test/nestjs-deep-imports.spec.ts`, which scans every
`@nestjs/<pkg>/<subpath>` import under `packages/kafka` and requires the
subpath to name a file (`.js` / `.ts` / `.d.ts`) inside the installed package.
It fails on both majors, which matters because on the 11.x install the trap is
otherwise invisible. Do not reach for
`@nestjs/common/interfaces/controllers/controller.interface` as a workaround —
that is still an internal path, and 12 defines the type as `object` anyway.

**Lifecycle-hook order across providers is not a contract.** NestJS 12
reordered lifecycle hooks (`onModuleInit`, `onApplicationBootstrap`,
`onModuleDestroy`, `beforeApplicationShutdown`, `onApplicationShutdown`) by
the component's level in the module hierarchy, so the order in which two
providers see the *same* hook differs between 11 and 12. What did not change
is the phase order — every `onModuleInit` still completes before any
`onApplicationBootstrap` — and that is the only sequencing this package relies
on: `KafkaRequestReplyService.onApplicationBootstrap` starts the reply
consumer knowing `KafkaProducerService.onModuleInit` has already connected the
shared producer. Nothing assumes an order among providers within a phase, and
nothing may start to: the explorer's "stop claiming, drain in-flight, then
disconnect" shutdown order lives inside its own `onApplicationShutdown`, not
across providers, and a change that needs another provider's same-phase hook
to have run first must express that as a dependency (inject it, or move the
work to an earlier phase) rather than as an assumption about hook sequencing.
No test asserts a within-phase hook order, and none should.

## Local Full-Mode Verification (optional infra + mutation testing)

Everything in this section is **opt-in and local-only**. Plain `npm test` and
CI's unit path run without Docker against the in-memory broker; forks work out
of the box. **CI never runs mutation testing** — it is an on-demand,
local-only gate.

### Gated integration suite (real KRaft Kafka)

- `npm run infra:up` — a disposable single-node KRaft broker from
  `compose.yaml` (`apache/kafka`, host port `127.0.0.1:19094`). Needs Docker.
  A fresh broker takes ~10-30 seconds to become healthy; `infra:up` waits on
  the container healthcheck before returning.
- `npm run test:full` — the unit suite first (in-memory broker, no env vars),
  then the real-broker integration suite with `KAFKA_BROKERS=localhost:19094`
  set on the integration half only. The integration spec proves what the
  in-memory broker cannot: a real produce → consume round-trip, a real
  transactional commit, and per-topic concurrency plus offset-commit
  durability.
- `npm run infra:down` — removes the container and volumes.
- The integration suite gates purely on `KAFKA_BROKERS`: point it at your own
  broker and run `npm run test:integration` instead.
- It needs the optional Confluent client installed once per checkout:
  `npm i --no-save "@confluentinc/kafka-javascript@^1.9"` — `--no-save`
  because it is an optional peer and must never land in `package.json` or the
  lockfile.

**AI agents working on this repo**: when Docker is available, run
`npm run infra:up && npm run test:full` before opening a PR that touches
package source, and report the result (including the integration suite) in
the PR body. When Docker is not available, run `npm test` and state that the
integration suite was skipped. Never wire any of this into CI.

### Mutation testing (Stryker — occasional targeted audit, local only, never in CI)

- `npm run test:mutation` — **incremental** run (cache:
  `reports/stryker-incremental.json`; only re-tests what changed). This is the
  pre-PR ritual for changes to package source.
- `npm run test:mutation:full` — every mutant from scratch (`--force`).
- `STRYKER_MUTATE='packages/kafka/kafka-dispatcher.ts,packages/kafka/kafka-backpressure.ts'`
  — comma-separated globs to scope a run to the files a change touched.
- `STRYKER_WITH_INFRA=1` — each mutant also runs the real-broker integration
  suite (`npm run test:mutant:full` per mutant, concurrency forced to 1
  because the specs share the one broker; run `npm run infra:up` first). Slow
  by design; use it when a change touches driver/broker-adjacent code.
- Report: `reports/mutation/mutation.html`. Thresholds are advisory
  (`break: null`) — the signal is *which mutants survive*, not the score.

**Occasional targeted audit, not a per-PR gate.** Run mutation testing
deliberately when you've reworked a file's logic — not on every PR. Scope
`STRYKER_MUTATE` to that one file, keep `--concurrency 2`, and verify a kill the
fast way: hand-apply the surviving mutation, run the plain suite, confirm your
new test fails, then `git checkout --` to revert. Full/unscoped runs re-test
every mutant against the whole suite and are slow to impractical — lean on
scoped runs plus hand-verification, and `kill -9` any leftover `stryker`
processes after a timeout. Treat survivors by the doctrine (add a test /
simplify redundant code / `// Stryker disable` a true equivalent / assert bounds
for timing). Keep CI fast and Docker-free — that is a deliberate contract.
