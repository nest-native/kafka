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
