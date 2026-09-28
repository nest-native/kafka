# Sample 03 — Headers, context, and error mapping

Demonstrates milestone 4: the parameter decorators, error mapping, and graceful
shutdown.

What it shows:

- `@KafkaMessage()` — the parsed message payload, mirroring `@Payload()`.
- `@KafkaHeaders('x-tenant')` — a single header by key. Header conventions stay
  neutral: the app picks `x-tenant`; the package never standardises keys.
- `@KafkaCtx()` — the raw `KafkaContext` (topic, partition, original message,
  headers), mirroring `@Ctx()`.
- Error mapping: a handler throwing a 4xx `BadRequestException` is committed by
  the default mapper, so a poison message is not redelivered forever. A
  transient error (a plain `Error` or a 5xx) is retried, with a backoff.
- Dead letters: `DeadLetterFilter` (`@UseFilters` on the handler) writes the
  rejected payment to `payments.captured.dlq` with `toDeadLetterMessage` — the
  original key, value, and headers plus Spring Kafka's `kafka_dlt-*` headers —
  before it is committed, so the poison message is kept rather than dropped. A
  second consumer reads the dead-letter topic back with `readDeadLetterHeaders`,
  and the smoke test asserts what it decoded.
- Graceful shutdown: `app.close()` stops accepting new claims, drains in-flight
  handlers, then disconnects.

## Run it

```bash
# in-memory loopback broker, no Kafka required
npm run test --workspace nest-native-kafka-sample-03-headers-context-errors
npm run start --workspace nest-native-kafka-sample-03-headers-context-errors
```

## Against a real broker

```bash
KAFKA_BROKERS=localhost:9092 \
  npm run start --workspace nest-native-kafka-sample-03-headers-context-errors
```

Setting `KAFKA_BROKERS` switches to Confluent's `@confluentinc/kafka-javascript`
client. Broker credentials must never be committed to sample code, logs, or docs.
