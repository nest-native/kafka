# Graceful Shutdown

On `app.close()` the transport shuts down in a defined order so no handler is
interrupted mid-message and no in-flight work is lost:

1. **Stop accepting new claims.** Consumers stop taking newly delivered messages.
2. **Drain in-flight.** The messages — and batches — already being processed run
   to completion.
3. **Disconnect.** Every consumer, and the producer, disconnect from the broker.

This ordering is part of the project's constitution and is covered by tests.

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
did not finish. A partition revoked during the drain keeps the progress already
made — see the rebalance-safe behavior in
[Batch & Concurrency](batch-and-concurrency.md).

## Testing It

`KafkaTestModule` runs the same shutdown path against the in-memory broker, so you
can assert drain behavior without a real cluster. See [Testing](testing.md).
