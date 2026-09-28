# Graceful Shutdown

On `app.close()` the transport shuts down in a defined order so no handler is
interrupted mid-message and no in-flight work is lost:

1. **Stop accepting new claims.** Every consumer is paused, so the client stops
   handing it records; records it had already fetched go back to the client
   instead of to a handler. Should one still arrive, it is handed back too — never
   acknowledged, because acknowledging a record is what gets it committed.
2. **Drain in-flight.** The messages — and batches — already being processed run
   to completion.
3. **Disconnect.** Every consumer, and the producer, disconnect from the broker.
   Only records a handler finished are committed; everything else is delivered
   to the partition's next owner.

This ordering is part of the project's constitution and is covered by tests,
including a real-broker case that shuts down mid-stream and requires the next
member of the group to receive every record the first one did not process.
Versions up to 0.5.1 acknowledged the records that arrived during the drain
instead of handing them back, so a record could be committed unprocessed on a
redeploy.

## Enabling Shutdown Hooks

Enable Nest's shutdown hooks for the drain to run on `SIGTERM` / `SIGINT`:

```ts
import {NestFactory} from '@nestjs/core';
import {AppModule} from './app.module';

async function bootstrap(): Promise<void> {
  const app = await NestFactory.create(AppModule);
  app.enableShutdownHooks();
  await app.listen(3000);
}

void bootstrap();
```

For a transport-only application created with `createMicroservice`, the same
`enableShutdownHooks()` call applies.

## Why It Matters

Combined with the rule that offsets commit only after a successful handler return,
graceful shutdown means a redeploy or scale-down never acknowledges a message it
did not finish. Work that is not finished is not committed, so the partition's
next owner receives it again — see how batches are resolved in
[Batch & Concurrency](batch-and-concurrency.md).

## Testing It

`KafkaTestModule` runs the same shutdown path against the in-memory broker, so you
can assert drain behavior without a real cluster. See [Testing](testing.md).
