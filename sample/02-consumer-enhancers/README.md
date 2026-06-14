# Sample 02 — Consumer decorators with the enhancer pipeline

Demonstrates milestone 3: `@KafkaConsumer` and `@KafkaHandler` running through the
full NestJS enhancer pipeline on the Kafka transport.

What it shows:

- A class-level `@KafkaConsumer('orders.placed', { groupId })` with a method-level
  `@KafkaHandler()`.
- A guard (`@UseGuards`) that blocks messages without a tenant.
- An interceptor (`@UseInterceptors`) that wraps the handler.
- A pipe (`@UsePipes`) that normalises the payload.
- An exception filter (`@UseFilters`) that catches a `BadRequestException` thrown
  by the pipe and suppresses it — mirroring how an RPC filter acknowledges a
  message.
- Constructor dependency injection into the consumer and the enhancers.

The enhancer classes are registered as providers, exactly as a NestJS app
registers guards/interceptors/pipes/filters used in `@UseX(SomeClass)`.

## Run it

```bash
# in-memory loopback broker, no Kafka required
npm run test --workspace nest-native-kafka-sample-02-consumer-enhancers
npm run start --workspace nest-native-kafka-sample-02-consumer-enhancers
```

## Against a real broker

```bash
KAFKA_BROKERS=localhost:9092 \
  npm run start --workspace nest-native-kafka-sample-02-consumer-enhancers
```

Setting `KAFKA_BROKERS` switches to Confluent's `@confluentinc/kafka-javascript`
client. Broker credentials must never be committed to sample code, logs, or docs.
