# Sample 01: Producer Basics

The smallest runnable NestJS app that wires `@nest-native/kafka`'s
`KafkaModule.forRoot()`, the `KafkaProducerService`, and a single handler that
logs every message it receives.

## What It Demonstrates

| Feature | File(s) |
| --- | --- |
| `KafkaModule.forRoot()` | `src/app.module.ts` |
| `KafkaProducerService.send` / `sendBatch` | `src/orders.service.ts` |
| One handler that logs messages | `src/logging-message.handler.ts` |
| Producer lifecycle (connect on init, disconnect on shutdown) | `scripts/smoke.ts` |
| Local smoke test (no broker required) | `scripts/smoke.ts` |

## Run

```bash
npm run start --workspace nest-native-kafka-sample-01-producer-basics
```

## Validate

```bash
npm run test --workspace nest-native-kafka-sample-01-producer-basics
```

## Local vs. Real Broker

By default the sample uses an in-memory loopback driver: every published message
is delivered straight to the logging handler. This keeps the smoke test runnable
with no Kafka broker and no native `librdkafka` install, matching the project's
"skip locally if env missing" contract.

Set `KAFKA_BROKERS` (a comma-separated broker list, e.g. `localhost:9092`) to
point the same app at a real cluster through Confluent's
`@confluentinc/kafka-javascript` client. A throwaway broker is easy to stand up:

```bash
docker run -d --name kafka -p 9092:9092 apache/kafka:3.9.0
KAFKA_BROKERS=localhost:9092 npm run start \
  --workspace nest-native-kafka-sample-01-producer-basics
```

## Why This Matters

This sample proves the milestone-2 surface end to end: the module wires a single
shared producer, the `KafkaProducerService` owns its connection lifecycle, and a
plain Nest provider receives and logs every message. The full `@KafkaConsumer` /
`@KafkaHandler` decorator pipeline lands in a later milestone and builds on this
same module shell.
