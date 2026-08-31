# ADR 0001: Request-reply

- **Status:** Proposed
- **Date:** 2026-08-31
- **Package version at time of writing:** 0.4.1

## Context

### Why this comes up now

`@nestjs/microservices`' `ClientKafka.send()` *is* request-reply. Every
application that uses `@MessagePattern` with the official Kafka transport
depends on a built-in correlation protocol: the client stamps a correlation id
and a reply address onto the request, the server sends the handler's return
value back to that address, and the client resolves the matching in-flight
call. `@nest-native/kafka` has no equivalent, which means those applications
**cannot migrate to this package at all** — and a documented migration path
from the official transport is one of this package's four headline
differentiators.

Worse, the migration guide's current answer is actively bad advice at scale.
[`docs/migration-from-nestjs-microservices.md`](../migration-from-nestjs-microservices.md)
says:

> If you relied on the transport's built-in request/reply correlation,
> implement it explicitly by producing to a reply topic with
> `KafkaProducerService` and correlating with a header you own.

The correlation part of that is easy. The part the guide waves past — making
the reply reach the *instance* that sent the request, across rebalances,
restarts, and N replicas behind a load balancer — is the genuinely hard part,
and it is exactly the kind of hand-rolling the community thread below
complains about. Telling users to build the one piece they cannot easily
build correctly is the opposite of this package's posture.

For honesty's sake, the external signal should not be overstated. In
[`nestjs/nest#13223`](https://github.com/nestjs/nest/issues/13223) (open since
2024, the "kafkajs seems unmaintained" thread this package exists because of),
the NestJS lead posted a copy-paste `@platformatic/kafka`
`CustomTransportStrategy` and noted it was "still missing support for
request-reply flow"; another participant replied that the snippet was not
production-ready. That exchange is about *his own snippet*, not about this
package, and nobody in that thread asked this package for anything. What it
is evidence of is narrower but still useful: the NestJS lead's own definition
of a complete Kafka transport replacement includes request-reply. The real
argument for the feature is the migration blocker above, not that thread.

### The honest case against

Kafka-as-RPC is a widely held anti-pattern, and for good reasons this ADR
does not intend to argue away:

- Kafka has no per-request addressing. Consumer groups distribute partitions,
  not replies, so "deliver this message to that process" has to be simulated
  on top of a log that was designed to do the opposite.
- A synchronous caller blocked on a log gets the worst of both worlds: HTTP's
  coupling with Kafka's latency floor (produce, fetch, group coordination)
  and none of HTTP's connection-level failure signals. There is no
  "connection refused" — only a timeout.
- The reply path quietly degrades Kafka's delivery model. Requests remain
  at-least-once, but a reply is only useful to a single in-memory promise in
  a single process; if that process died, the reply is noise. Request-reply
  over Kafka is therefore **at-most-once on the reply path** no matter how it
  is built, and pretending otherwise would be a lie.

The migration guide's `@MessagePattern` vs `@EventPattern` section takes an
explicit position: "`@nest-native/kafka` models Kafka as the event log it
is." Shipping request-reply amends a documented stance, and this ADR treats
that as a real cost, not a technicality. The resolution is in the Decision:
the *default* stays the event log — `@KafkaHandler` remains fire-and-forget,
and nothing about request-reply is ambient. What changes is that the package
stops delegating the one unsafe piece of the migration to its users.

### The repo's own precedent

The constitution already decided a structurally similar question once:
`GUIDELINES_NEST_KAFKA.md` §3 rules out "a DLQ 'framework' — provide
primitives, document the pattern." The DLQ decision works because the
primitives genuinely suffice: a DLQ is one `producer.send()` inside an
`errorMapper`, and every failure mode of that produce is the application's
to own anyway. Request-reply does not fit that shape. Its primitives
(produce, consume, headers) are already exported, and composing them is
where the correctness bugs live: reply-consumer readiness races on the first
request, reply loss under rebalance, orphaned correlation state, timeout vs
late-reply races, and interop with the official transport's header protocol.
A "pattern" writeup cannot make those guarantees testable for the user;
a shipped implementation with 100% branch coverage can. So the precedent
cuts the other way here — but its instinct (resist becoming a platform)
bounds this design everywhere below: one routing strategy, single replies
only, no streaming, no scatter-gather, no admin tooling.

### What the official transport actually does (baseline for interop)

Because interop with partially migrated systems is a design input, the
official mechanics matter. As of NestJS 11, `ClientKafka`:

- derives one reply topic per request pattern (`<pattern>.reply`) and
  requires `client.subscribeToResponseOf(pattern)` boilerplate before
  connecting;
- joins a consumer group over those reply topics and relies on a custom
  partition assigner (`KafkaReplyPartitionAssigner`) to keep each client
  instance's reply-partition assignment sticky across rebalances;
- stamps three headers on the request — `kafka_correlationId`,
  `kafka_replyTopic`, `kafka_replyPartition` — advertising a partition the
  instance currently owns, and **throws at send time** if the instance owns
  no partition of the reply topic (more replicas than partitions means some
  replicas cannot call `send()` at all);
- resolves replies by correlation id, treating the presence of
  `kafka_nest-err` as an error and `kafka_nest-is-disposed` as completion.

`ServerKafka` (`@MessagePattern`) reads the reply address from those request
headers and produces the handler result to that exact topic and partition;
requests without a reply address are treated as events.

Two facts about that design shape everything below. First, its instance
affinity depends on a custom JavaScript partition assigner — a feature
`kafkajs` offers and **the Confluent client cannot express**: librdkafka
accepts only its built-in assignors (`range`, `roundrobin`,
`cooperative-sticky`); there is no hook for a user-supplied assignment
strategy in `@confluentinc/kafka-javascript`'s KafkaJS-compat surface. The
official design is not portable to this package's client even if we wanted
it. Second, even *with* the sticky assigner, a rebalance between send and
reply can hand the advertised partition to a different instance, which then
consumes and discards the reply while the requester waits forever (the
official client has no default timeout). Reply loss under rebalance is a
live failure mode of the design we are replacing, not a hypothetical.

## Decision

Request-reply ships in `@nest-native/kafka` as an **opt-in migration
bridge**, in the next 0.x minor. Seven decisions follow, in decreasing order
of consequence. Everything here is a design commitment; the implementation
PR builds from it and any deviation comes back through this document.

### 1. Framing: opt-in bridge, fire-and-forget default

- `@KafkaHandler` stays fire-and-forget. A handler replies only when it
  declares `reply: true` (mirroring the existing `batch: true` idiom — one
  handler decorator, mode flags, no second decorator).
- The client side lives on a dedicated injectable,
  `KafkaRequestReplyService`, and does nothing — starts no consumer, touches
  no topic — unless `KafkaModule.forRoot({ requestReply: { … } })` is
  configured. Configuration is the opt-in.
- The documentation frames it as what it is: a bridge for migrating
  `@MessagePattern`/`ClientKafka.send()` code and for the narrow cases where
  a Kafka round-trip is genuinely wanted, with the degraded semantics
  (at-most-once replies, timeout means *unknown*, see §5) stated up front.
  The migration guide's "models Kafka as the event log it is" position is
  rewritten, not deleted: the default model is unchanged; the escape hatch
  is now supported instead of hand-rolled.

Why not reject the feature outright: the package promises a migration path,
and today that promise excludes every `@MessagePattern` user — the exact
population the package was built for. Why not primitives-only: argued in
Context; the value is the tested composition, and an untestable design (or a
design whose testing burden lands on users) fails this repo's own bar.

### 2. Reply routing — shared reply topic, one ephemeral consumer per instance

This is the decision that needed an ADR. The requirement: a reply must reach
the *process* that sent the request — not its consumer group, one specific
instance — under rebalance, scale-out/in, crash, and N replicas behind a
load balancer, on a client with no custom partition assigners.

#### Options considered

**(a) Per-instance reply topic, unique name per boot** (e.g.
`orders.replies.<uuid>`). Each instance consumes only its own topic; nothing
is shared, so rebalance is a non-issue.

- *Correctness:* perfect — a single-member group over a topic nobody else
  reads.
- *Sprawl:* disqualifying. Topics outlive processes. A 10-replica deployment
  restarting daily mints ~3,650 topics a year; each carries partitions,
  metadata, and controller state. Cleanup requires `DeleteTopics` rights and
  only ever runs on *graceful* shutdown — every crash leaks a topic
  permanently.
- *Cold start:* first use depends on topic auto-creation
  (`auto.create.topics.enable`), which hardened and managed clusters
  commonly disable; explicit creation would drag an admin-client surface
  into the package.

**(a′) Per-instance reply topic, stable identity** (user-supplied instance
id, e.g. the pod name: `orders.replies.orders-7f6d9-2`). Fixes the sprawl by
bounding topics to the historical replica ceiling; restarts reuse the topic.

- *Correctness:* good, with one wrinkle — identity reuse after a crash can
  replay stale replies to a fresh process; starting at latest offsets plus
  correlation filtering absorbs that.
- *Costs:* the package cannot conjure a stable instance identity portably —
  the user must wire one in, which is real configuration burden for every
  deployment rather than an advanced option. Still leaks topics on
  scale-down. Still assumes topic creation rights or auto-create.
- This is the runner-up. It is strictly the better shape for
  high-throughput request-reply because it eliminates the fan-out cost of
  the chosen option; see "what would change the choice."

**(b) One shared reply topic; every instance consumes all of it and filters
by correlation id.** Each instance runs one additional consumer with a
unique, ephemeral `groupId` (`<replyTopic>-<uuid>`), subscribed from latest,
never committing offsets. A reply is delivered to every instance; the one
holding the matching pending entry resolves it; the other N−1 drop it on a
map miss.

- *Correctness under rebalance:* there is nothing to rebalance. Every group
  has exactly one member, so group membership changes on other instances
  cannot move a reply away from its requester. This is the only option
  whose correctness argument is a *absence* of machinery rather than more of
  it.
- *Instance death mid-request:* the pending promise dies with the process
  that owned it — there is nobody to deliver to, which is the correct
  outcome. The dead instance's group is empty with no committed offsets and
  the broker expires it (offset/group retention, default 7 days). Nothing
  else leaks.
- *N replicas behind a load balancer:* works unconditionally. No coupling
  between replica count and partition count — the reply topic can have 1
  partition or 60; neither matters.
- *Sprawl:* one topic, provisioned once, forever. The only residue is
  short-lived group metadata.
- *Cold start:* the reply consumer starts at application bootstrap (config
  is the opt-in, so eager start is paid only by apps that opted in). A
  brand-new group's first join eats the broker's
  `group.initial.rebalance.delay.ms` (default 3 s), so the *first*
  `request()` after boot can wait up to roughly that long for readiness;
  subsequent requests pay only the produce-fetch round trip.
- *The real cost — duplicate fan-out:* every reply is fetched N times for N
  replicas. Concretely: 10 replicas × 200 replies/s × 1 KiB ≈ 2 MiB/s of
  redundant broker egress and client-side filtering. At the volumes where
  that number hurts, request-reply over Kafka is the wrong tool regardless
  of routing strategy, and the docs will say so with this arithmetic in
  them.

**(c) Shared reply topic, dedicated partition per instance** — the official
transport's design. Rejected on three independent grounds:

1. **Unimplementable as designed on this client.** Its stickiness depends on
   a custom assigner; librdkafka exposes only built-in assignors. A variant
   without stickiness (advertise whatever partition the instance currently
   owns) makes every group change a reply-loss event.
2. **Couples operations to scale.** Partitions must be ≥ replicas or some
   replicas cannot issue requests at all (the official client throws exactly
   there). Repartitioning a topic to scale a *stateless service* out is an
   operational absurdity this package would be signing users up for.
3. **Loses in-flight replies on rebalance** even in the official, sticky
   implementation, as described in Context. Adopting a design whose known
   failure mode is "the reply vanishes and the caller hangs" would recreate
   the class of bug (`#12355`-adjacent) this package exists to fix.

**(d) Direct partition assignment, no group** (raw `assign()`): would make
(b) cheaper on the broker (no group metadata at all), but the Confluent
client's KafkaJS-compat surface — the only surface this package's driver
models — does not expose group-less assignment. Noted as an internal
optimization if the client ever grows it; not a design fork.

**(e) Out-of-band reply channels** (HTTP callback, Redis): violates
"Kafka has no HTTP coupling" and transport-only scope. Not seriously
entertained.

#### The choice

**Option (b).** One shared reply topic per application, named explicitly by
the user in `requestReply.replyTopic`; one ephemeral single-member consumer
per instance; correlation-id filtering. It is the only option that is
simultaneously correct under every rebalance scenario, implementable on the
Confluent client, free of topic/partition/replica coupling, and free of
cleanup obligations. Its weakness — duplicate fan-out — is linear,
predictable, and documented with numbers, which is the kind of weakness this
project prefers over machinery whose failure mode is silent reply loss.

The reply topic is **provisioned by the user, like any other topic this
package touches**. No admin client, no auto-creation logic. The docs
recommend: any partition count (it is irrelevant — start with 1),
`retention.ms` in the minutes range (e.g. 300000; replies older than the
longest timeout are garbage by definition), `cleanup.policy=delete`. ACL
note for locked-down clusters: requesters need READ on the reply topic and
group `<replyTopic>-*`; repliers need WRITE on whatever reply topics their
callers name.

The reply consumer never commits offsets and subscribes from latest —
committed offsets for an ephemeral group are pure `__consumer_offsets`
churn, and the correlation map, not the log position, is the source of
truth. This is a deliberate asymmetry with the package's rebalance-safe
handler path and it is documented as such: replies are not durable work,
they are volatile signals to an in-memory promise.

**Readiness is contractual.** `request()` must never produce a request
before this instance's reply consumer is assigned and fetching — otherwise
a fast reply can land before the consumer's "latest" position is
established and be skipped forever. The service awaits readiness internally
(bounded by `readinessTimeoutMs`, default 10000 ms, failing with an error
that names the reply topic — which is also how a missing/unauthorized reply
topic surfaces instead of as a silent hang). The detection mechanism
(polling the client's `assignment()`, a rebalance callback, or a sentinel
self-message) is left to the implementer; the guarantee is not, and the
real-broker suite must prove it for the first-request-after-boot case.

**Runner-up and what would change the choice.** (a′) per-instance stable
topics. Because the reply *address travels in the request's headers*, the
client protocol is strategy-agnostic: a future
`requestReply.strategy: 'instance-topic'` could ship in a minor without
breaking anything — repliers already send wherever the header says. The
trigger to build it would be a measured deployment where the fan-out
arithmetic above actually bites (sustained high reply volume across ≥ ~20
replicas) and which cannot move that call path off Kafka. Client-side
group-less `assign()` appearing in the Confluent compat surface would
likewise slim (b)'s broker footprint without changing the topology.

### 3. API surface

New exports, shown as the signatures the implementation must match.

**Module opt-in** (extends `KafkaModuleOptions`):

```ts
export interface KafkaModuleOptions {
  // …existing options…

  /**
   * Opt into request-reply. Absent (the default), no reply consumer is
   * created and KafkaRequestReplyService.request() rejects immediately with
   * a configuration error.
   */
  requestReply?: KafkaRequestReplyOptions;
}

export interface KafkaRequestReplyOptions {
  /**
   * The reply topic this application's instances share. Provisioned by you,
   * like any topic this package consumes. Required — there is no derived
   * default, because the topic is infrastructure you own.
   */
  replyTopic: string;

  /** Default per-request timeout. @default 30000 */
  timeoutMs?: number;

  /**
   * How long request() may wait for this instance's reply consumer to be
   * assigned and fetching before failing fast. @default 10000
   */
  readinessTimeoutMs?: number;

  /**
   * Prefix for the ephemeral per-instance consumer group id; a UUID is
   * appended per process. @default `${replyTopic}-`
   */
  groupIdPrefix?: string;

  /** Override the header key contract (see §4). Defaults interoperate with
   *  `@nestjs/microservices`. */
  headers?: Partial<KafkaRequestReplyHeaderKeys>;

  /** Advanced passthrough to the reply consumer (same shape and routing as
   *  every other consumer config in this package). */
  consumer?: KafkaConsumerConfig;
}
```

**Client side:**

```ts
@Injectable()
export class KafkaRequestReplyService {
  /**
   * Produce `record.message` to `record.topic` with a correlation id and
   * this instance's reply address stamped into its headers, then resolve
   * with the correlated reply or reject on timeout/abort/remote error.
   */
  request<T = unknown>(
    record: KafkaRequestRecord,
    options?: KafkaRequestOptions,
  ): Promise<KafkaReply<T>>;
}

export interface KafkaRequestRecord {
  topic: string;
  /** Same shape send() takes: the caller serializes `value` explicitly,
   *  exactly as for every other produce in this package. */
  message: KafkaProducerMessage;
}

export interface KafkaRequestOptions {
  /** Overrides requestReply.timeoutMs for this call. */
  timeoutMs?: number;
  /** Cancels the wait (not the remote work — see §5). */
  signal?: AbortSignal;
}

export interface KafkaReply<T> {
  /** Deserialized like every consumed payload: JSON when it parses, the
   *  decoded string otherwise, null for a tombstone. */
  value: T;
  headers: KafkaMessageHeaders;
  correlationId: string;
  topic: string;
  partition: number;
  offset?: string;
}
```

The method is named `request`, not `sendAndReceive`: `send` in this package
already means "publish and return broker acks," and a second `send*` verb
with wholly different semantics invites the exact confusion the
producer/consumer split avoids today.

Errors it rejects with (all exported, all extending `Error`):

```ts
export class KafkaReplyTimeoutError extends Error {
  readonly topic: string;        // the request topic
  readonly correlationId: string;
  readonly timeoutMs: number;
}

/** The reply arrived and is an error reply: the remote handler failed and
 *  its error mapped to 'commit' (§5). */
export class KafkaReplyRemoteError extends Error {
  readonly remote: unknown;          // decoded error header content
  readonly reply: KafkaReply<unknown>;
}

export class KafkaReplyAbortedError extends Error {}
```

An abort rejects with `signal.reason` when the caller provided one,
`KafkaReplyAbortedError` otherwise. Producer failures while sending the
request propagate untouched, as `send()`'s do. Calling `request()` with
`requestReply` unconfigured rejects with an `Error` naming the missing
option. `request()` rejects user-supplied request headers that collide with
the configured reserved keys (correlation id, reply topic/partition) rather
than silently overwriting them.

**Consumer side** (extends `KafkaHandlerOptions`):

```ts
export interface KafkaHandlerOptions extends KafkaConcurrencyOptions {
  groupId?: string;
  batch?: boolean;

  /**
   * Reply with the handler's resolved return value when the consumed
   * request carries a reply address in its headers. Fire-and-forget
   * remains the default. Incompatible with `batch: true`.
   * @default false
   */
  reply?: boolean;
}
```

```ts
@KafkaConsumer(undefined, { groupId: 'orders-service' })
export class OrdersTotalConsumer {
  @KafkaHandler('orders.total', { reply: true })
  async total(@KafkaMessage() query: TotalQuery): Promise<TotalResult> {
    return this.orders.total(query.customerId); // becomes the reply
  }
}
```

Semantics of the reply step, in pipeline order:

- The reply value is the handler's **post-enhancer** result: interceptors
  wrap and may transform it, observables collapse via the existing
  `lastValueFrom` normalization (which also means streamed replies are
  structurally excluded, not merely unsupported), and a value returned by an
  exception filter that handled the error becomes the reply. This mirrors
  `@MessagePattern` exactly.
- Serialization of the return value: `string`, `Buffer`, and `null` pass
  through; `undefined` becomes a `null` value; everything else is
  `JSON.stringify`ed. (The producer API stays explicit-serialization;
  the transport serializes here because the handler has no seam to.)
- The reply message's key is the correlation id (deterministic, greppable);
  its headers carry the correlation id, the completion marker, and — on the
  error path — the error header. Request headers are *not* echoed onto the
  reply; propagating tracing context is the application's interceptor's
  job, per the package's header neutrality.
- A `reply: true` handler consuming a message with **no reply address** in
  its headers runs normally and skips the reply step with a debug-level
  log. Mixed traffic on a request topic (e.g. replayed requests nobody is
  waiting on) is legitimate.
- Replies are produced through the module's shared producer, so
  `KafkaTestModule` sees them and graceful shutdown ordering is unchanged.

Bootstrap-time validation (same style as the existing missing-topic error):
`reply: true` with `batch: true` is a configuration error; more than one
`reply: true` handler routed to the same topic anywhere in the application
is a configuration error (two repliers means duplicate replies; the second
would always lose the correlation race — refusing to start beats a
heisenbug).

Integration points, so the implementer knows the blast radius: a `reply`
flag threaded through `DiscoveredHandler`/`DispatchHandler`, the reply
produce in the dispatcher's success and error paths (it already holds the
raw message and context), the new service plus an internal reply-consumer
class, and the option interfaces. No changes to the enhancer runtime, the
params resolver, or the driver interface.

### 4. Correlation and headers

A request-reply protocol cannot be header-neutral — the address and the
correlation id have to live somewhere with agreed names. The package's
documented neutrality (`kafka-context.ts`: no standardized
`traceId`/`correlationId`/`messageType` keys) is **preserved for general
messaging and amended for this opt-in feature only**: header keys used by
request-reply are part of its contract, and they are configurable.

The defaults are `@nestjs/microservices`' own keys:

```ts
export interface KafkaRequestReplyHeaderKeys {
  correlationId: string;  // 'kafka_correlationId'
  replyTopic: string;     // 'kafka_replyTopic'
  replyPartition: string; // 'kafka_replyPartition'
  error: string;          // 'kafka_nest-err'
  disposed: string;       // 'kafka_nest-is-disposed'
}
```

This is the interop question, and it matters more than aesthetics (the
`kafka_` names are nobody's favourite). The realistic migration is partial:
service A moves to this package while services B and C stay on the official
transport for months. With the official keys as the default, both directions
work without either side knowing the other changed:

- **This package calls an un-migrated `@MessagePattern` service.**
  `request()` stamps `kafka_replyTopic: <our shared topic>` and
  `kafka_correlationId`; it deliberately omits `kafka_replyPartition`, which
  `ServerKafka` treats as "no partition targeting" and produces to the
  default partitioner — any partition of our reply topic is fine, we
  consume them all. The reply's `kafka_nest-err` presence maps to
  `KafkaReplyRemoteError`; the disposed marker is ignored (one reply
  resolves the promise — a streaming `@MessagePattern` observable resolves
  our promise with its first emission and the rest are dropped as late
  replies; calling streaming patterns through this bridge is documented as
  unsupported).
- **An un-migrated `ClientKafka` calls a handler migrated to
  `reply: true`.** The old client keeps its own `<pattern>.reply` topics and
  *does* advertise a `kafka_replyPartition` (it throws client-side without
  one). Our reply step honours both headers verbatim — topic and explicit
  partition — and sets the correlation id and the disposed marker so the old
  client's observable completes. No topology change on the old side, no
  `subscribeToResponseOf` anywhere on ours.

Correlation ids are `crypto.randomUUID()` (`node:crypto` — the zero-runtime-
dependency rule holds). The pending entry is registered *before* the request
is produced so a same-tick reply cannot race the map.

One honesty rule binds this whole section: the exact byte-level conventions
of the official transport (what `kafka_nest-err` contains for various error
shapes, what value marks disposal) are **pinned by contract tests against
the installed `@nestjs/microservices`**, not by this ADR's prose or anyone's
memory of Nest internals. Our own error replies carry
`JSON.stringify({ name, message })` in the error header and a `null` value;
what we *accept* is presence-based (any non-absent error header is an error
reply), which tolerates both worlds. If the contract tests contradict a
detail above (e.g. `ServerKafka` rejecting a missing reply-partition
header), the tests win and this ADR gets a dated correction.

Teams wanting clean keys on a green field configure
`requestReply.headers` and lose nothing but interop they did not need.

### 5. Failure semantics

The organizing principle: **the requester owns retry policy; the broker owns
redelivery; a timeout means *unknown outcome*, never "it didn't happen."**
Request-reply narrows Kafka's at-least-once to at-most-once on the reply
path, and every rule below follows from refusing to hide that.

| Failure | Surfaces as | Why |
| --- | --- | --- |
| No reply within `timeoutMs` | `KafkaReplyTimeoutError` | The only signal Kafka can give. The request may still be processed later — the error message says so. |
| No consumer group on the request topic | Timeout (indistinguishable from slow) | Kafka has no cheap "is anyone subscribed" signal; pretending to detect this (admin lookups) would be racy theatre. |
| `signal` aborted | `signal.reason` (or `KafkaReplyAbortedError`) | Cancels the *wait*, not the remote work — stated in the docs; there is no cross-process cancellation to offer honestly. |
| Handler throws, `errorMapper` → `'commit'` | Error reply → requester rejects with `KafkaReplyRemoteError` | A non-retryable failure is an answer; the caller should get it now, not at timeout. Offset commits as today. |
| Handler throws, `errorMapper` → `'retry'` | No reply; broker redelivers; a later success within the window resolves the request normally, otherwise timeout | Preserves this package's error-mapping contract unchanged: `commit` means "done" (so the caller learns how it ended), `retry` means "not done yet" (so no reply exists to send). A transient failure can self-heal inside the timeout window. |
| Reply produce fails (broker down, reply topic missing on the replier's side) | The produce error, wrapped in `KafkaReplyDeliveryError`, fed to the `errorMapper` (default: retry) | From the requester's view an unsent reply is unprocessed work; redelivery is another chance to answer within the window. The wrapper exists so custom mappers can branch on it — and because an unroutable reply (topic that will never exist) would otherwise retry forever, which the docs call out with the mitigation (provision topics; map `KafkaReplyDeliveryError` to `'commit'`). |
| Reply address present but unparseable (garbage partition value) | Reply skipped, warning logged, offset **committed** | Statically undeliverable — retrying can never succeed, so retrying is the one wrong answer (a poison-request partition blocker). |
| Reply topic missing/unauthorized on the requester's side | `request()` fails fast when readiness is not reached within `readinessTimeoutMs`, naming the topic | Turns the worst diagnostic (silent hang) into the best one (an error with the topic name in it). |
| Duplicate reply (redelivered handler run, competing replier) | First settles the promise; rest dropped by the map miss | At-most-once resolution is the only coherent promise semantics. |
| Late reply (after timeout/abort) | Dropped, debug-level log | It is expected steady-state noise under `'retry'` mapping; a warning would cry wolf. |
| Reply for another instance's correlation id | Dropped silently — no log | This is the *hot path* of the chosen routing ((N−1)/N of consumed replies); logging it would be self-DoS. By design, stated in the docs. |
| Application shutdown with requests pending | Pending requests reject with `KafkaReplyAbortedError`; new `request()` calls reject immediately | Draining "until the reply arrives" could hold shutdown for the full timeout for work whose outcome is unknowable anyway; failing fast honours the existing stop-claims → drain → disconnect order. |

Behavioural delta for migrators, stated in the migration guide: the official
transport error-replies on *every* handler failure; here a `'retry'`-mapped
error (the default for non-4xx) redelivers server-side first, so an
un-migrated caller's fast-fail becomes a timeout unless the retry succeeds.
Restoring official behaviour is one line — an `errorMapper` returning
`'commit'` for the affected topics. Also new: a default timeout *exists*
(30 s); the official client's `send()` waits forever unless the app added
its own `timeout` operator.

### 6. Testing strategy

The 100%-branch gate makes "hard to test" equal to "wrongly designed," so
testability was a design input above (eager readiness, injectable clock-free
timeouts, presence-based header parsing).

**What the in-memory broker already covers** — deliberately, this design
needed almost nothing from it. `InMemoryKafkaBroker` delivers every produce
to every subscribed consumer regardless of group, which is *exactly* the
chosen routing's semantics (option (b) is "consume everything, filter by
correlation id"), headers already round-trip through `toConsumed`, and
`idle()` already settles request → handler → reply cascades. The full unit
matrix runs Docker-free: happy path through the whole enhancer pipeline,
interceptor-transformed replies, filter-produced replies, error replies
(`'commit'` path), no-reply-plus-redelivery (`'retry'` path — the test
re-emits, as redelivery is the broker's job), missing reply address on a
`reply: true` handler, header-collision rejection, unconfigured `request()`,
both bootstrap validation errors, duplicate/late/foreign replies (tests emit
crafted messages straight onto the reply topic — no new broker API needed),
abort, shutdown rejection, and timeouts (Node 20+'s `node:test` mock timers,
or millisecond-scale real timeouts; implementer's choice, no sleeps).

**In-memory broker changes actually required:** one, small — per-message
delivery currently invents the partition from the message *index* and
ignores `message.partition`; honouring an explicit `message.partition` (as
batch delivery already does) is needed so reply-partition targeting
(the old-`ClientKafka` interop path) is assertable in-memory. Worth doing as
an honesty fix regardless.

**What genuinely requires the real-broker suite** (`KAFKA_BROKERS`-gated,
same as today):

1. **Cross-process round trip:** two application contexts on one broker —
   requester in one, `reply: true` handler in the other — proving the
   ephemeral-group topology on real group coordination, including the
   first-request-after-boot readiness guarantee (produce the request as
   early as the API allows; the reply must not be lost to a
   latest-offset race).
2. **Restart during a pending request** (`KAFKA_RESTART_CONTAINER`-gated,
   extending the existing restart case): a request in flight across a broker
   restart either completes or times out — never hangs past its deadline,
   never resolves with a wrong correlation.
3. **Instance-affinity under load:** three requester instances issuing
   interleaved requests; every reply resolves in the instance that asked.
4. **Interop contract tests** — the tests §4 defers to: a real `ServerKafka`
   `@MessagePattern` app answering our `request()`, and a real `ClientKafka`
   calling our `reply: true` handler. `@nestjs/microservices` is already a
   required peer; its Kafka transport needs `kafkajs`, which enters
   **devDependencies only** — the irony of dev-depending on the library this
   package exists to escape is acknowledged and accepted, because pinning
   interop by test beats pinning it by folklore, and nothing published
   changes (`"dependencies": {}` holds).

Mutation-testing posture per the constitution: an occasional targeted audit
of the new correlation/timeout logic (`STRYKER_MUTATE` scoped to the new
service file) once it stabilizes — not a per-PR gate.

### 7. Scope boundary for v1 of the feature

Ships:

- `KafkaRequestReplyService.request()` with timeout, abort, and the error
  classes above.
- `requestReply` module options; eager reply consumer with the readiness
  guarantee; shared-topic/ephemeral-group routing — **one strategy**.
- `@KafkaHandler(topic, { reply: true })` with post-enhancer reply value,
  addressed by request headers, error mapping per §5.
- Default header contract interoperable with `@nestjs/microservices`,
  configurable keys.
- The in-memory-broker partition fix; the unit matrix; the four real-broker
  cases; interop contract tests.
- Docs: a `website/docs/request-reply.md` page (semantics, the fan-out
  arithmetic, topic provisioning and ACLs, the at-most-once honesty), the
  rewritten `@MessagePattern` section and new mapping rows in the migration
  guide (`@MessagePattern('t')` → `@KafkaHandler('t', { reply: true })`;
  `client.send('t', v)` → `request()`; `subscribeToResponseOf` → nothing),
  and a `GUIDELINES_NEST_KAFKA.md` §12 entry recording this decision plus
  the §5 header-neutrality amendment — in the same PR as the code, per the
  repo's rule that a stance change and its enforcement land together.
- A focused sample (`sample/07-request-reply`) and the showcase's migration
  scenario extended — in a **separate PR**, per the contributing rule
  separating sample and library PRs.

Does NOT ship, with reasons:

- **Streamed/multiple replies** (the official observable protocol). The
  single-promise contract is the honest one for at-most-once replies;
  `kafka_nest-is-disposed` bookkeeping for streams roughly doubles the
  protocol surface for a pattern even the official docs barely use.
  Structurally excluded by the observable normalization, revisit only on
  concrete demand.
- **A second routing strategy** (per-instance topics). Additive later by
  design (§2); shipping two strategies at once means shipping two
  half-tested ones.
- **Scatter-gather / broadcast requests.** First-reply-wins is the
  documented behaviour if multiple groups answer; aggregation is an
  application concern.
- **Requester-side automatic retries.** The caller got an error or a
  timeout; re-sending is its decision. Transport-level retries on top of an
  unknown-outcome timeout manufacture duplicates.
- **Admin/topic-creation tooling.** The reply topic is provisioned
  infrastructure, same as every topic this package consumes.
- **Reply-topic allowlists on the replier.** The reply address comes from
  inbound headers, which is a confused-deputy shape — but the enforcement
  layer for "where may this service write" is broker ACLs, which exist and
  are documented, not an application-level list that duplicates them badly.
  Revisit if real deployments demonstrate an ACL-shaped gap.
- **Metrics/observability hooks** for dropped or late replies. The package
  imposes no logging shape anywhere else; debug logs are the v1 answer.
- **A `ClientProxy`-compatible adapter.** Migration ergonomics, not
  permanent dual-runtime support — the same reasoning that rules out a
  kafkajs shim.

## Consequences

- Applications on `@MessagePattern`/`ClientKafka.send()` can migrate, which
  they categorically could not before; partially migrated fleets
  interoperate in both directions with default configuration.
- The migration guide stops recommending that users hand-roll the one piece
  of this feature that is unsafe to hand-roll.
- A documented stance is amended: the event-log model remains the default
  and `@KafkaHandler` remains fire-and-forget, but the package now carries
  an explicitly bounded RPC bridge and the docs burden of keeping its
  degraded semantics impossible to miss.
- The reply path is at-most-once and the docs say so; anyone needing
  guaranteed delivery of results is pointed back at events.
- Reply fan-out costs N× broker egress under the chosen routing; the
  arithmetic is documented, and the strategy seam (§2) is the pre-planned
  exit if it ever bites in practice.
- New public surface (one service, three option blocks, four error classes,
  one handler flag) joins the 100%-coverage and complexity-≤15 gates;
  `"dependencies": {}` is unchanged; `kafkajs` and nothing else enters
  dev-only scope, and its addition goes through the mandatory supply-chain
  review like any dependency change.
- Release lands as the next 0.x minor with the full version-sync ritual
  (samples, README `Status:` literals, roadmap, changelog).

## Deliberately left open for the implementer

- The readiness-detection mechanism (§2): assignment polling, rebalance
  callback, or sentinel — the guarantee and its integration test are fixed,
  the mechanism is not.
- Timer mechanics for timeouts (mock-timer tests vs short real timeouts;
  whether the pending-request timer is `unref`ed) — bounded by "no sleeps in
  tests" and "no hung shutdowns."
- Exact byte-level interop details (§4) — pinned by the contract tests, with
  this ADR corrected if they disagree.
- Internal decomposition (dispatcher-owned vs runner-owned reply step),
  bounded by the complexity gate and the integration points listed in §3.
- Log placement and exact messages for the debug/warn paths in §5.

## Rejected alternatives (summary)

| Alternative | Why rejected |
| --- | --- |
| Do nothing; keep the "implement it yourself" guide text | Leaves the package's stated migration promise false for every `@MessagePattern` user, and delegates the unsafe part (instance routing under rebalance) to the people least equipped to test it. |
| Primitives-only, DLQ-style (export a correlation map + helpers, document the pattern) | The value *is* the assembled guarantee (readiness, routing, timeout races, interop); primitives cannot make that guarantee testable for users. The DLQ precedent holds where primitives suffice — here they demonstrably do not. |
| Per-instance UUID reply topics | Unbounded topic sprawl on restart churn; permanent leaks on every crash; deletion rights and admin surface required to mitigate. |
| Per-instance stable-identity reply topics | Runner-up (§2). Rejected as *default* for mandatory identity configuration and scale-down leaks; kept as the pre-planned second strategy if fan-out cost is measured to matter. |
| Shared topic + partition-per-instance (the official design) | Requires a custom partition assigner the Confluent client cannot express; couples partition count to replica count; loses in-flight replies on rebalance even in the original. |
| Shared topic + shared group + advertised own partition | The previous row minus stickiness: every group change is a reply-loss event. |
| Group-less `assign()` consumption | Not exposed by the client's KafkaJS-compat surface the driver models; noted as a future internal optimization only. |
| Observable-returning `send()` mirroring `ClientKafka` | A promise is the honest shape for a single at-most-once reply; streams import the disposed-marker protocol for marginal value. `from(promise)` exists. |
| Auto-reply whenever a reply address is present (no `reply: true`) | Violates the fire-and-forget default this design commits to, and turns inbound headers into an ambient instruction to produce — a confused-deputy default. |
| Clean new header keys as the default | Breaks both interop directions for partially migrated fleets — the population this feature exists for. Available via configuration instead. |
| HTTP/Redis side-channel replies | Not Kafka, not transport-only, not this package. |
