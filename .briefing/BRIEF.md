# nest-kafka-native — Implementation Brief

> Single source of truth for implementing `nest-native/nest-kafka-native`.
> Read this end-to-end before writing code. It is written for a fresh
> session that has no other context.

**Project type:** New package.
**Repository:** https://github.com/nest-native/nest-kafka-native
**Org:** https://github.com/nest-native

---

## 1. Read these first

This package's constitution is `.briefing/AI_CODING_GUIDELINES.md` at the repo
root. Read it end-to-end before writing code. It is the SOLE governing
document for this package — no other guideline files apply.

This brief specifies WHAT to build and ON WHAT SCHEDULE. The constitution
specifies HOW. If they conflict on a general nest-native principle,
**the constitution wins**.

If you discover an inconsistency between this brief and the constitution,
or between either and the implementation reality (e.g., the Confluent
client API differs from what we assumed), you may update the constitution
as part of your PR:

- Update in a focused commit on the current branch (alongside the code
  that exposed the inconsistency).
- The PR body MUST include a "Guideline Updates" section quoting the
  before/after of every changed section.
- Do not weaken the Security, Release Sync, or Cognitive Complexity
  sections without explicit operator instruction here in the brief.
- Do not delete sections; rewrite or add a "Superseded" note.
- If a change would require revisiting a previously-merged milestone,
  STOP and flag — do not silently invalidate prior work.

## 2. Mission

A decorator-first NestJS Kafka integration on top of Confluent's officially
supported `@confluentinc/kafka-javascript` client. Preserve
`@MessagePattern` / `@EventPattern` ergonomics so users coming from
`@nestjs/microservices` can migrate, while solving the correctness gaps
the kafkajs-based official transport has accumulated.

## 3. Community pain (the gap)

`@nestjs/microservices`'s Kafka transport is built on `kafkajs`, which the
community widely treats as effectively unmaintained — see
`nestjs/nest#13223` (52+ comments; Confluent staff offered their client as
the replacement). The official transport also has unresolved correctness
issues: sequential per-topic processing (`#12703`), rebalance hangs
(`#12355`), exception swallowing (`#9679`).

Confluent's `@confluentinc/kafka-javascript` (v1.9+, ~1.8M monthly
downloads) is stable and offers a KafkaJS-compatible API. The migration
path is well-defined; what is missing is the Nest-native decorator-first
wrapper.

Evidence:
- https://github.com/nestjs/nest/issues/13223
- https://github.com/nestjs/nest/issues/12703
- https://github.com/nestjs/nest/issues/12355
- https://github.com/nestjs/nest/issues/9679
- https://github.com/confluentinc/confluent-kafka-javascript
- https://farmijo.net/blog/nestjs-kafja-custom-transport/

## 4. Non-goals

- **Replacing `@nestjs/microservices` wholesale.** This is a Kafka
  transport, not a transport framework.
- **Permanent kafkajs compatibility shim.** Migration ergonomics: yes.
  Permanent dual-runtime support: no.
- **Bundling a schema registry client.** A `nest-confluent-schema-registry`
  sub-module may follow once this is stable. Out of v1.
- **AsyncAPI generation.** Belongs in `nest-asyncapi-native`.
- **Stream-processing primitives (Faust / Kafka Streams).** Producer/
  consumer integration only.

## 5. Tech stack and versions

| Item | Choice |
| --- | --- |
| Node | `>=20` |
| NestJS | `11.x` |
| TypeScript | `^6` |
| `@confluentinc/kafka-javascript` | `^1.9` (peer) |
| Validation | both class-validator and Zod (per guideline files) |
| Test runner | `node:test` + `c8` (mirror `nest-drizzle-native`) |
| Lint | ESLint 10 + SonarJS, complexity threshold 15 |
| Package manager | `npm@11` |

Published package keeps `"dependencies": {}`. Confluent client and Nest
packages go in `peerDependencies`. NON-NEGOTIABLE per the project's
constitution.

## 6. Repo layout

Mirror the existing nest-native package layout exactly. Use
`nest-native/nest-trpc-native` as the concrete template (npm workspaces,
`packages/kafka/`, `sample/`, `website/`, `scripts/`, the three eslint
config files, the same CI/release scripts).

## 7. Public API surface (proposed — confirm via samples first)

Module:
- `KafkaModule.forRoot(options)` / `forRootAsync(options)`
- `KafkaModule.forFeature([HandlerClass])`

Decorators:
- `@KafkaConsumer('topic-or-pattern', options?)` — class-level
- `@KafkaHandler('topic', options?)` — method-level, mirrors
  `@MessagePattern` / `@EventPattern` semantics
- `@KafkaMessage()` — parameter, parsed message payload
- `@KafkaHeaders()` — parameter, headers
- `@KafkaContext()` — parameter, raw transport context

Producer:
- `@InjectKafkaProducer()` for direct producer access
- `KafkaProducerService` with `send`, `sendBatch`, `transactional` helpers

Testing:
- `KafkaTestModule` with in-memory transport for unit tests
- Driver-backed integration tests gated on CI Kafka service (skip
  locally if env missing, per the "driver samples stay app-owned"
  decision in `.briefing/AI_CODING_GUIDELINES.md` §12)

Enhancer pipeline integration is NON-NEGOTIABLE: `@UseGuards`,
`@UseInterceptors`, `@UsePipes`, `@UseFilters` must work on handler
methods.

## 8. v1 scope discipline

**Ships:** `KafkaModule.forRoot`/`forRootAsync`, consumer decorators with
full enhancer support, producer service (single + batch + transactional),
header/context parameter decorators, error-mapping helpers, `KafkaTestModule`,
one showcase sample + four+ focused samples, CI parity with existing repos.

**Does NOT ship:** Schema Registry integration, exactly-once helpers beyond
what the client provides, a DLQ "framework" (provide the primitives,
document the pattern), AsyncAPI generation.

## 9. Design questions to settle in v1

Document the answers in the package's `.briefing/AI_CODING_GUIDELINES.md` before the
first substantive PR:

1. **Rebalance-safe consumption.** In-flight messages must complete or be
   explicitly aborted; offsets must commit only after successful handler
   return.
2. **Backpressure.** Cap in-flight messages per handler. Default and how
   it surfaces.
3. **Per-topic concurrency.** `#12703` is specifically about sequential
   processing. The new transport must address this with a documented
   default and an opt-out.
4. **Graceful shutdown.** Consumers stop accepting new claims, drain
   in-flight, then disconnect.
5. **Header conventions.** Stay neutral on `traceId` / `correlationId` /
   `messageType`; do not standardize keys.

## 10. Quality gates

Mirror `nest-trpc-native`'s `npm run ci` shape: typecheck, lint,
complexity:check, test:cov (target 100% per existing precedent),
release:check, security:audit, sample. Plus: a driver-backed integration
test against a real Kafka in CI via GH-Actions service container, skipped
locally when `KAFKA_BROKERS` env is missing.

## 11. Milestones

1. **Bootstrap.** Repo skeleton matching `nest-trpc-native` exactly. Empty
   package. CI green. Tag `v0.0.1-scaffold`. (`.briefing/AI_CODING_GUIDELINES.md`
   is already in the repo from the initial commit; no need to create it.)
2. `KafkaModule.forRoot()` + producer service. One handler that logs
   messages. Smoke test against a local Kafka container.
3. `@KafkaConsumer` + `@KafkaHandler` with full enhancer pipeline. Showcase
   sample passes guards/interceptors/pipes/filters.
4. Header + context parameter decorators. Error mapping. Graceful shutdown.
5. Batch consume + per-topic concurrency. Address `#12703` / `#12355`
   explicitly in a sample + test.
6. Transactional producer helper.
7. `KafkaTestModule` + mock helpers. Migration guide from
   `@nestjs/microservices` Kafka transport.
8. Documentation site (Docusaurus, mirror existing pattern). Release v0.1.

## 12. First-session checklist

1. Read `.briefing/AI_CODING_GUIDELINES.md` in full (the constitution).
2. Read this brief end-to-end.
3. Confirm latest published versions of `@confluentinc/kafka-javascript`
   and the existing nest-native packages on npm.
4. Use `nest-native/nest-trpc-native` as the concrete template for
   `CONTRIBUTING.md`, `SECURITY.md`, `.github/PULL_REQUEST_TEMPLATE.md`,
   `dependabot.yml`, eslint configs, `tsconfig.base.json`, release scripts.
5. Stand up workspace + empty package + CI. Push. Confirm green.
6. Stop at `v0.0.1-scaffold` and hand back.

## 13. Definition of done for v1

- Every issue scenario from `#13223`, `#12703`, `#12355`, `#9679` has a
  regression test proving the new transport behaves correctly.
- Migration guide from `@nestjs/microservices` Kafka exists, validated by
  porting one sample.
- `npm run ci` green on Node 20 and 22.
- Driver-backed integration test passes in CI against a real Kafka.
- Published package has `"dependencies": {}`.

## 14. Honest risks

- **librdkafka binary distribution.** Alpine, Windows, ARM64 are each
  potential install-time landmines. Test the install matrix in CI early.
- **Confluent API churn.** The client tracks librdkafka. Pin to a major
  and document the upgrade contract.
- **Migration friction.** kafkajs error handling differs subtly from
  confluent. Document every behavioral delta.
- **Scope creep into "Kafka platform."** Resist Schema Registry, ksqlDB,
  Connect, Streams. v1 is producer/consumer only.

## 15. References

- This project's constitution: `.briefing/AI_CODING_GUIDELINES.md`.
- Existing nest-native packages as concrete templates:
  - https://github.com/nest-native/nest-drizzle-native
  - https://github.com/nest-native/nest-trpc-native
