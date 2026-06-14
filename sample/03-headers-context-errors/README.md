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
  the default mapper, so a poison message is acknowledged instead of redelivered
  forever. A transient error (a plain `Error` or a 5xx) is retried — supply your
  own mapper through `KafkaModule.forRoot({ errorMapper })` to override, for
  example to route a failure to a dead-letter topic before committing.
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
