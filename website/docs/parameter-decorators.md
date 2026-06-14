# Parameter Decorators

Instead of the positional `(payload, context)` arguments, decorate individual
parameters — mirroring `@Payload()` / `@Ctx()` from `@nestjs/microservices`. The
decorators participate in the enhancer pipeline, so param-level pipes run just as
they do on an HTTP controller argument.

```ts
import {ParseIntPipe} from '@nestjs/common';
import {
  KafkaConsumer,
  KafkaContext,
  KafkaCtx,
  KafkaHandler,
  KafkaHeaders,
  KafkaMessage,
  KafkaMessageHeaders,
} from '@nest-native/kafka';

@KafkaConsumer('orders.placed')
export class OrdersConsumer {
  @KafkaHandler()
  handle(
    @KafkaMessage() order: OrderPlaced, // whole parsed payload
    @KafkaMessage('id') id: string, // one payload property
    @KafkaHeaders() headers: KafkaMessageHeaders, // all headers (empty if none)
    @KafkaHeaders('trace-id') traceId: string | Buffer, // one header by key
    @KafkaCtx() context: KafkaContext, // topic, partition, raw message, headers
  ): void {}
}
```

## The Decorators

| Decorator | Resolves to |
| --- | --- |
| `@KafkaMessage()` | The whole deserialized payload. |
| `@KafkaMessage('prop')` | One property of the payload. |
| `@KafkaHeaders()` | All headers as a `KafkaMessageHeaders` map (empty if none). |
| `@KafkaHeaders('key')` | One header value (`string \| Buffer`). |
| `@KafkaCtx()` | The raw `KafkaContext`. |
| `@KafkaBatch()` | The raw `KafkaConsumerBatch` (batch handlers only). |

`@KafkaCtx()` is the parameter form; `KafkaContext` is also the type of the second
positional argument and is returned by `ExecutionContext.switchToRpc().getContext()`.

## The Context Object

`KafkaContext` exposes the transport details without leaking Confluent client
internals:

- `getTopic()` — the source topic.
- `getPartition()` — the source partition.
- `getMessage()` — the raw incoming message (key, value, headers, offset).
- `getHeaders()` — the message headers.

## Header Conventions Stay Neutral

The package does not standardize `traceId` / `correlationId` / `messageType`
keys. Read whatever keys your producers set with `@KafkaHeaders('your-key')`.
Treat header values as untrusted input and validate them like any payload —
secrets must never travel in headers shown in samples, logs, or docs.

## Batch Handlers

In batch mode, `@KafkaMessage()` resolves to the array of deserialized payloads
and `@KafkaBatch()` resolves to the raw `KafkaConsumerBatch`. See
[Batch & Concurrency](batch-and-concurrency.md).
