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

## Dead-Letter Queues Are A Pattern, Not A Framework

The package provides the primitives, not a DLQ framework. Implement the pattern in
a filter or a custom mapper: produce the failed message to a dead-letter topic,
then commit so it is not redelivered:

```ts
KafkaModule.forRoot({
  client: {brokers: ['localhost:9092']},
  errorMapper: async (error, context) => {
    await deadLetterProducer.send({
      topic: `${context.getTopic()}.dlq`,
      messages: [{value: JSON.stringify({error: String(error)})}],
    });
    return 'commit';
  },
});
```

Sample `03-headers-context-errors` isolates the error-mapping behavior end to end.
See the [Sample Catalog](samples/catalog.md).
