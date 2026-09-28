# Error Mapping

When a handler throws and no `@UseFilters` exception filter handles it, the
transport maps the error to consumer behavior instead of swallowing it. This is
the direct answer to [`nestjs/nest#9679`](https://github.com/nestjs/nest/issues/9679),
where the official transport quietly dropped exceptions.

## The Default Policy

`defaultKafkaErrorMapper` classifies the error and returns a
`KafkaErrorBehavior` of `'commit'` or `'retry'`:

- A 4xx `HttpException` (for example `BadRequestException`) is a non-retryable
  client error, so the offset is **committed** — a poison message is acknowledged
  instead of being redelivered forever.
- Any other error — a 5xx `HttpException`, an `RpcException`, or an arbitrary
  thrown value — is treated as transient and **retried**: the offset is left
  uncommitted so the broker redelivers.

Offsets commit only after a handler returns successfully, so a `'retry'` simply
means the offset is never advanced for that message.

## Retries Back Off

Left to itself, Confluent's client redelivers a `'retry'`-mapped message by
seeking back to it, and the next fetch hands it over again — measured at a flat
~0.5 s apart on a real broker, forever. During an outage that is two calls a
second per stuck partition against whatever is already failing.

So the transport backs off. When a message (or a batch) fails with `'retry'`, it
pauses **that partition** for a delay, lets the client seek back, and resumes
the partition when the delay is up. The delay starts at 1 s and doubles with
every consecutive failure of the same message, up to 30 s; it starts over once
the partition handles something, or a different message fails. The consumer's
other partitions keep flowing meanwhile — the backoff never sleeps in the
handler path, which would hold the worker every partition shares.

```ts
KafkaModule.forRoot({
  client: {brokers: ['localhost:9092']},
  retryBackoff: {initialDelayMs: 500, multiplier: 2, maxDelayMs: 60_000},
  // retryBackoff: false — redeliver immediately, as the client does on its own
});
```

The backoff never gives up on a message: turning a retry into a commit is the
mapper's decision, not the transport's. It needs a driver that can pause and
resume partitions; the Confluent driver can, and with a custom driver that
cannot, retries redeliver immediately. Streaks live in memory, per consumer — a
rebalance or a restart starts a failing message over at the initial delay.

Because a `'retry'` sends no reply, a replying handler that keeps failing now
takes longer to answer; the caller's timeout still bounds its wait. See
[Request-Reply](request-reply.md).

## Filters Run First

Only errors that escape the handler's `@UseFilters` exception filters reach the
mapper. An application can acknowledge any error — or route it somewhere — by
catching it in a filter. This keeps the Nest model intact: the filter is the
first line of defense, and the mapper is the transport's fallback.

## Custom Mapping

Override the policy with your own mapper on `KafkaModule.forRoot`:

```ts
KafkaModule.forRoot({
  client: {brokers: ['localhost:9092']},
  errorMapper: (error, context) => (isFatal(error) ? 'commit' : 'retry'),
});
```

The mapper receives the error and the `KafkaContext` (or `KafkaBatchContext` for
batch handlers, together typed as `KafkaErrorContext`), so it can decide based on
the topic, partition, or headers.

The mapper may be **async**. The transport awaits it before the message is
committed or handed back, so work inside it — a dead-letter produce — finishes
first. A mapper that throws or rejects leaves the message to be retried, so a
dead-letter produce that fails never loses it. Declare an async mapper's return
type: TypeScript otherwise widens a returned `'commit'` to `string`, which the
option does not accept.

## Dead-Letter Queues Are A Pattern, Not A Framework

The package provides the primitives, not a DLQ framework: it builds and reads
dead-letter records, and your application decides which failures go where.

`toDeadLetterMessage(context, error)` turns a failed message into its dead
letter: the original key, value, and headers, plus the headers Spring Kafka's
`DeadLetterPublishingRecoverer` writes — `kafka_dlt-original-topic`,
`-original-partition`, `-original-offset`, `-original-timestamp`,
`-original-consumer-group`, `kafka_dlt-exception-fqcn`, `-exception-message`, and
`-exception-stacktrace`. The names and the encodings are Spring's (the
partition is a 4-byte integer, the offset and timestamp 8-byte integers, the
rest UTF-8), so a dead-letter topic written here reads the same as one written
by a JVM service, and Spring tooling can read ours. `readDeadLetterHeaders`
decodes them back; `toDeadLetterMessages` does the same for every message of a
failed batch.

The Nest-native place to produce the dead letter is an exception filter — it is
DI-enabled, so it can inject `KafkaProducerService`:

```ts
@Injectable()
@Catch(BadRequestException)
export class DeadLetterFilter implements ExceptionFilter {
  constructor(private readonly producer: KafkaProducerService) {}

  async catch(error: BadRequestException, host: ArgumentsHost): Promise<void> {
    const context = host.switchToRpc().getContext<KafkaContext>();
    await this.producer.send({
      topic: `${context.getTopic()}.dlq`,
      messages: [toDeadLetterMessage(context, error, {consumerGroup: 'billing'})],
    });
  }
}

@KafkaConsumer('orders', {groupId: 'billing'})
export class OrdersConsumer {
  @UseFilters(DeadLetterFilter)
  @KafkaHandler()
  handle(@KafkaMessage() order: Order): void {
    // throw new BadRequestException(...) for an order that can never succeed
  }
}
```

The filter handles the error, so the message is committed — but only after the
`send` it awaits: the transport awaits a filter, and one that rejects (the
dead-letter topic is unavailable) leaves the message to be retried instead of
losing it. Catch only the errors a retry cannot fix — here, the same 4xx class
the default mapper would otherwise commit silently — and let transient ones
through to the mapper and its [backoff](#retries-back-off).

An async error mapper works the same way when a producer is in reach — it is
awaited, and a rejection retries — but a mapper is module configuration, not a
provider, so it cannot inject one; `deadLetters` below is a producer you
created yourself:

```ts
errorMapper: async (error, context): Promise<KafkaErrorBehavior> => {
  if (defaultKafkaErrorMapper(error, context) === 'retry') return 'retry';
  await deadLetters.send({
    topic: `${context.getTopic()}.dlq`,
    messages:
      context instanceof KafkaBatchContext
        ? toDeadLetterMessages(context, error)
        : [toDeadLetterMessage(context, error)],
  });
  return 'commit'; // only once the dead letter is written
},
```

A stack trace names files and code paths; pass `{includeStackTrace: false}`
when the dead-letter topic is readable outside the team that owns the code.

Versions up to 0.5.1 compared the mapper's result without awaiting it, so an
async mapper committed every message it saw, before its produce finished and
even when the produce failed. They also shipped no dead-letter record builder,
and the example here forwarded only `String(error)` — the original message was
lost.

Sample `03-headers-context-errors` isolates the error-mapping behavior end to end.
See the [Sample Catalog](samples/catalog.md).
