# Sample 07 — Request-reply (the `@MessagePattern` bridge)

Demonstrates the opt-in request-reply bridge: a `@KafkaHandler` that answers,
a `KafkaRequestReplyService` that asks, and the failure semantics that make the
whole thing honest.

Request-reply over Kafka is a bridge for migrating off `@nestjs/microservices`,
not the model this package recommends. Read
[`website/docs/request-reply.md`](../../website/docs/request-reply.md) — in
particular "When this is the wrong tool" — before reaching for it.

What it shows:

- **Two independent opt-ins.** `src/totals.consumer.ts` answers with
  `@KafkaHandler(topic, { reply: true })` and needs *no* module configuration —
  a replier learns where to answer from the request's own headers.
  `src/app.module.ts` configures `requestReply.replyTopic`, which is the entire
  client-side opt-in: without it no reply consumer exists and `request()`
  rejects naming the missing option.
- **Fire-and-forget is untouched.** `notifyPlaced` sits on the same consumer
  class with no flag and produces no reply. Request-reply is never ambient.
- **The reply is the handler's return value**, post-enhancer, serialized by the
  transport (the one place this package serializes for you, because a handler
  has no seam to do it itself).
- **A failure the caller learns about immediately.** A 4xx maps to `'commit'` —
  "done" — so it comes back as an error reply and rejects with
  `KafkaReplyRemoteError` instead of waiting out the timeout. A
  `'retry'`-mapped failure would send no reply and let the broker redeliver.
- **A timeout means _unknown_.** `totalFromNobody` asks a topic nobody consumes;
  the only signal Kafka can give is a timeout, and the error says so in as many
  words.
- **Shutdown fails fast.** Pending and new requests reject rather than holding
  shutdown open for work whose outcome is unknowable anyway.
- **`KafkaTestModule`.** `scripts/smoke.ts` runs the entire feature — reply
  consumer, readiness handshake, correlation, timeouts, error replies — against
  the in-memory broker, and inspects the reply message the handler produced with
  `broker.getSentTo(...)`. No sleeps, no real Kafka, no native `librdkafka`.

The properties an in-memory broker cannot show — real group coordination for the
per-instance reply groups, reply affinity across three concurrent instances, a
request spanning a broker restart, and byte-level interop with a real
`@nestjs/microservices` `ServerKafka` and `ClientKafka` — are covered by the
package's `KAFKA_BROKERS`-gated integration suite.

## Run it

```bash
# in-memory KafkaTestModule, no Kafka required
npm run test --workspace nest-native-kafka-sample-07-request-reply
npm run start --workspace nest-native-kafka-sample-07-request-reply
```

## Against a real broker

```bash
KAFKA_BROKERS=localhost:9092 \
  npm run start --workspace nest-native-kafka-sample-07-request-reply
```

Setting `KAFKA_BROKERS` switches to Confluent's `@confluentinc/kafka-javascript`
client. The reply topic (`sample-07.replies`) is infrastructure you provision,
like every topic this package consumes: one partition is plenty, with
`retention.ms` in the minutes range. Broker credentials must never be committed
to sample code, logs, or docs.
