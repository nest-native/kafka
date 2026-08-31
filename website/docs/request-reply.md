# Request-Reply

Request-reply is an **opt-in migration bridge**, not the model this package
recommends. `@KafkaHandler` stays fire-and-forget and Kafka stays the event log
it is. What this adds is the one piece a `@MessagePattern` /
`ClientKafka.send()` application cannot safely hand-roll: making a reply reach
the **instance** that asked, across rebalances, restarts, and N replicas behind
a load balancer.

Read the next section before the API. If it changes your mind, that is the
section doing its job.

## When this is the wrong tool

Kafka-as-RPC is a widely held anti-pattern, and this page is not going to argue
it away.

- **Kafka has no per-request addressing.** Consumer groups distribute
  partitions, not replies. "Deliver this message to that process" has to be
  simulated on top of a log designed to do the opposite.
- **A caller blocked on a log gets the worst of both worlds:** HTTP's coupling
  with Kafka's latency floor (produce, fetch, group coordination) and none of
  HTTP's connection-level failure signals. There is no "connection refused" —
  only a timeout.
- **The reply path is at-most-once.** Requests stay at-least-once, but a reply
  is only useful to one in-memory promise in one process. If that process died,
  the reply is noise. Nothing here pretends otherwise.
- **A timeout means the outcome is _unknown_** — never "it did not happen". The
  request is still on the topic and may be processed after your call has
  already failed.

Use it when you are **migrating** off `@nestjs/microservices`' Kafka transport
and need `@MessagePattern` semantics to keep working, or for the narrow cases
where a Kafka round trip is genuinely what you want. If you are designing
something new and you need a synchronous answer, use HTTP or gRPC. If you need
the result to be durable, use an event and a second topic.

## Turning it on

Two independent opt-ins, and you rarely need both in the same application.

**To issue requests**, configure `requestReply` on the module. Configuration
*is* the opt-in: absent it, no reply consumer is created, no topic is touched,
and `request()` rejects with an error naming the missing option.

```ts
KafkaModule.forRoot({
  clientId: 'orders-api',
  client: {brokers: ['localhost:9092']},
  requestReply: {
    // Provisioned by you, like every topic this package consumes.
    replyTopic: 'orders-api.replies',
  },
});
```

```ts
@Injectable()
export class OrdersFacade {
  constructor(private readonly requests: KafkaRequestReplyService) {}

  async total(customerId: string): Promise<TotalResult> {
    const reply = await this.requests.request<TotalResult>({
      topic: 'orders.total',
      message: {value: JSON.stringify({customerId})},
    });
    return reply.value;
  }
}
```

**To answer requests**, add `reply: true` to a handler. This side needs *no*
module configuration at all: a replier learns where to answer from the request's
own headers.

```ts
@Injectable()
@KafkaConsumer(undefined, {groupId: 'orders-service'})
export class OrdersTotalConsumer {
  @KafkaHandler('orders.total', {reply: true})
  async total(@KafkaMessage() query: TotalQuery): Promise<TotalResult> {
    return this.orders.total(query.customerId); // this becomes the reply
  }
}
```

The flag mirrors the existing `batch: true` idiom — one handler decorator, mode
flags, no second decorator. `reply: true` and `batch: true` together is a
configuration error (a batch has no single request to answer), and so is routing
two replying handlers to the same topic (two repliers answer every request
twice, and the second always loses the correlation race). Both are refused at
bootstrap rather than shipped as a heisenbug.

## The reply topic is yours to provision

There is no admin client and no auto-creation here, exactly as for every other
topic this package consumes.

| Setting | Recommendation |
| --- | --- |
| Partitions | Any. Start with `1` — the count is irrelevant to routing (see below). |
| `retention.ms` | Minutes, e.g. `300000`. A reply older than your longest timeout is garbage by definition. |
| `cleanup.policy` | `delete` |

On a locked-down cluster: requesters need `READ` on the reply topic and on
consumer groups matching `<replyTopic>-*`; repliers need `WRITE` on whatever
reply topics their callers name.

If the topic is missing or unreadable, `request()` fails inside
`readinessTimeoutMs` with an error naming the topic — rather than hanging, which
is the worst diagnostic there is.

## How a reply finds your instance, and what it costs

One shared reply topic. Each **instance** runs one extra consumer in a
single-member consumer group of its own (`<replyTopic>-<uuid>`), subscribed from
latest, committing no offsets. Every reply is delivered to every instance; the
one holding the matching correlation id resolves its promise, and the rest drop
theirs on a map miss.

The correctness argument is an *absence* of machinery: **a group of one has
nothing to rebalance.** Membership changes on other instances cannot move a
reply away from the process that asked for it, scaling out or in changes
nothing, and the reply topic's partition count is decoupled from your replica
count entirely.

The official transport instead gives each client a dedicated partition of the
reply topic. That design needs a custom partition assigner, which `librdkafka`
cannot express; it forces `partitions >= replicas` (the official client throws
at `send()` when an instance owns no partition); and it still loses in-flight
replies when a rebalance moves the advertised partition mid-request. See
[ADR 0001](https://github.com/nest-native/kafka/blob/main/docs/adr/0001-request-reply.md)
for the full comparison.

**The cost is duplicate fan-out, and it is linear.** Every reply is fetched once
per replica:

> 10 replicas x 200 replies/s x 1 KiB ≈ **2 MiB/s** of redundant broker egress,
> plus the client-side filtering to drop 9 of every 10 messages.

Multiply your own numbers the same way. If the answer alarms you, that is a
signal about request-reply over Kafka rather than about this routing strategy —
at those volumes the round trip belongs on HTTP or gRPC. The strategy is a seam:
a per-instance-topic strategy can be added in a minor release without changing
the wire protocol, because the reply address travels in the request's headers.

### Readiness before the first request

`request()` never produces before this instance's reply consumer is proven to be
fetching. A consumer that starts at *latest* would skip a reply that landed
before its position was established, so the service round-trips a valueless
sentinel through the reply topic at startup and waits for it to come back. A
brand-new consumer group also pays the broker's
`group.initial.rebalance.delay.ms` on its first join, so the first `request()`
after boot can be slower than the ones after it.

## Timeouts, cancellation, and errors

```ts
const reply = await this.requests.request<TotalResult>(
  {topic: 'orders.total', message: {value: JSON.stringify(query)}},
  {timeoutMs: 5_000, signal: controller.signal},
);
```

- `timeoutMs` defaults to `30000` and can be overridden per call.
- `signal` cancels **the wait, not the remote work.** There is no cross-process
  cancellation to offer honestly; the handler keeps running and its reply is
  dropped when it arrives.

The rejections, all exported:

| Error | Means |
| --- | --- |
| `KafkaReplyTimeoutError` | No reply in time. **Outcome unknown.** Carries `topic`, `correlationId`, `timeoutMs`. |
| `KafkaReplyRemoteError` | The remote handler failed and its error mapped to `'commit'` — a final answer. Carries the decoded `remote` payload and the `reply`. |
| `KafkaReplyAbortedError` | The wait was cancelled by your `signal` (when it carries no reason of its own) or by application shutdown. |
| `KafkaReplyDeliveryError` | Raised on the **replying** side when the reply itself could not be produced. It goes through your `errorMapper` like any other handler failure. |

A producer failure while sending the request propagates untouched, exactly as
`send()`'s does. There are no transport-level retries: retrying an unknown
outcome manufactures duplicates, so re-sending is your decision.

### How handler failures reach the caller

The `errorMapper` contract is unchanged by this feature, and it decides what the
caller sees:

- `'commit'` means *done*, so the failure is a final answer: an error reply goes
  out and the caller rejects with `KafkaReplyRemoteError` immediately.
- `'retry'` means *not done yet*, so **no reply is sent**. The broker redelivers
  and a later success still resolves the original request inside its timeout
  window. If none succeeds in time, the caller times out.

That is a behavioural delta from the official transport, which error-replies on
every handler failure. Restoring the old behaviour is one line — an
`errorMapper` returning `'commit'` for the affected topics. See
[Error Mapping](error-mapping.md).

:::warning A plain `throw new Error()` makes your caller wait out the timeout

The default mapper sends anything that is not a 4xx to `'retry'`, so the most
natural line to write in a replying handler is also the one that produces no
reply at all:

```ts
// The caller waits the full timeout, then learns "outcome unknown" — for a
// failure that was permanent and knowable the moment it happened.
throw new Error('orgId must be a number');

// A 4xx maps to 'commit': the offset advances and the failure travels back as
// an error reply, so the caller rejects with KafkaReplyRemoteError at once.
throw new BadRequestException('orgId must be a number');
```

The rule of thumb: if redelivering the *same* message could never succeed, it is
a 4xx. Validation failures, unknown ids and malformed payloads are permanent;
a database that is momentarily down is not.

Watch for this in tests too. A test that only asserts "no answer came back"
passes under either path, so it can go green by timing out while claiming to
prove the error reply. Bound the elapsed time, or assert on the error type.
:::

### The rest of the failure table

| Situation | What happens |
| --- | --- |
| Nobody consumes the request topic | A timeout, indistinguishable from slow. Kafka has no cheap "is anyone subscribed" signal, and faking one with admin lookups would be racy theatre. |
| Duplicate reply (redelivery, competing replier) | The first settles the promise; the rest are dropped. |
| A reply arriving after the timeout or abort | Dropped, logged at debug. Under `'retry'` mapping this is expected steady-state noise. |
| A reply for another instance | Dropped silently, no log. This is the hot path of the routing — `(N-1)/N` of everything consumed — and logging it would be self-DoS. |
| A request with a reply address this replier cannot parse | The reply is skipped, a warning is logged, and the offset **commits**. A statically undeliverable address can never become deliverable, so retrying would only block the partition. |
| Shutdown with requests pending | Pending requests reject with `KafkaReplyAbortedError` and new calls reject immediately. Draining could hold shutdown for a full timeout on work whose outcome is unknowable anyway. |

## Interoperating with `@nestjs/microservices`

The default header keys are the official transport's own, so a partially
migrated fleet works in both directions with no configuration on either side:

| Key | Purpose |
| --- | --- |
| `kafka_correlationId` | Correlates a reply to its request |
| `kafka_replyTopic` | Where to answer |
| `kafka_replyPartition` | Read (never written) — honours an old client's explicit partition |
| `kafka_nest-err` | Present on an error reply |
| `kafka_nest-is-disposed` | Marks the reply as final, so an old client's observable completes |

- **This package calling an un-migrated `@MessagePattern` service.**
  `request()` stamps the correlation id and reply topic, and deliberately omits
  `kafka_replyPartition`: `ServerKafka` treats a missing value as "no partition
  targeting", and this package reads every partition of its reply topic anyway.
- **An un-migrated `ClientKafka` calling a `reply: true` handler.** The old
  client keeps its own `<pattern>.reply` topics and its explicit reply
  partition; the replying handler honours both verbatim and sets the completion
  marker so the old client's observable emits and completes. No
  `subscribeToResponseOf` anywhere on this side, and no topology change on the
  other.

Both directions are pinned by contract tests that run a **real** `ServerKafka`
and a **real** `ClientKafka` against a real broker, rather than by this page's
prose. Streaming `@MessagePattern` handlers (multiple emissions) are not
supported through this bridge: the first emission resolves the promise and the
rest are dropped as late replies.

Green-field teams who want none of this can rename the keys:

```ts
requestReply: {
  replyTopic: 'app.replies',
  headers: {correlationId: 'x-correlation-id', replyTopic: 'x-reply-to'},
}
```

Nothing is lost but interop you did not need. Header neutrality elsewhere in the
package is unchanged: this is a bounded exception for one opt-in feature, and
`KafkaContext.getHeaders()` still returns the raw map untouched.

## What the reply value is

On the replying side, the reply is the handler's **post-enhancer** return value:
interceptors wrap and may transform it, observables collapse to their last
value, and a value returned by an exception filter that handled the error
becomes the reply. This mirrors `@MessagePattern` exactly.

Serialization is the one place this package serializes for you, because a
handler has no seam to do it itself: `string`, `Buffer`, and `null` pass
through, `undefined` becomes a `null` value, and everything else is
`JSON.stringify`ed. On the requesting side, `reply.value` is deserialized like
every consumed payload — JSON when it parses, the decoded string otherwise.

A `reply: true` handler that consumes a message with **no** reply address runs
normally and skips the reply step with a debug log. Mixed traffic on a request
topic — a replayed request nobody is waiting on, an event on a shared topic — is
legitimate.

## Testing

`KafkaTestModule`'s in-memory broker runs the whole thing without Docker:
requests, replies, correlation, timeouts, and error replies. Replies go through
the module's shared producer, so the broker records them and
`broker.idle()` settles the request → handler → reply cascade.

```ts
const module = await Test.createTestingModule({
  imports: [
    KafkaTestModule.forRoot({
      clientId: 'test',
      requestReply: {replyTopic: 'app.replies'},
    }),
  ],
  providers: [OrdersTotalConsumer],
}).compile();
```

See [Testing](testing.md).

## Not shipped, on purpose

Streaming or multiple replies; scatter-gather and response aggregation;
requester-side automatic retries; a second routing strategy; a
`ClientProxy`-compatible adapter; admin/topic-creation tooling; reply-topic
allowlists (broker ACLs are the enforcement layer for "where may this service
write"). The reasoning for each is in
[ADR 0001](https://github.com/nest-native/kafka/blob/main/docs/adr/0001-request-reply.md).
