# Testing

You can test the whole transport without a real broker. `KafkaTestModule` runs
the producer service, the `@KafkaConsumer` enhancer pipeline, batch consumption,
transactions, and graceful shutdown against an in-memory `InMemoryKafkaBroker` —
no native `librdkafka`, no `KAFKA_BROKERS` env.

## `KafkaTestModule`

Swap `KafkaModule` for `KafkaTestModule` in a testing module:

```ts
import {Test} from '@nestjs/testing';
import {
  InMemoryKafkaBroker,
  KAFKA_TEST_BROKER,
  KafkaTestModule,
} from '@nest-native/kafka';

const moduleRef = await Test.createTestingModule({
  imports: [KafkaTestModule.forRoot(), OrdersModule],
}).compile();
await moduleRef.init(); // fires onApplicationBootstrap; consumers subscribe

const broker = moduleRef.get<InMemoryKafkaBroker>(KAFKA_TEST_BROKER);

// Inject a message straight to a consumer...
await broker.emit('orders.placed', {value: JSON.stringify({id: '1'})});
// ...or produce through a service and assert on what the broker recorded:
expect(broker.getSentTo('orders.placed')).toHaveLength(1);

await moduleRef.close();
```

`KafkaTestModule.forRoot(options?)` / `forRootAsync(options?)` accept the same
options as `KafkaModule` (minus `driverFactory`, which is fixed to the in-memory
broker) plus a `broker` option to reuse an existing `InMemoryKafkaBroker`. Inject
the broker with the `KAFKA_TEST_BROKER` token or `@InjectKafkaTestBroker()`.

## The In-Memory Broker

`InMemoryKafkaBroker` is the loopback transport the test module runs on:

- `emit(topic, message)` injects a consumed message into a subscribed consumer.
- `getSent()` / `getSentTo(topic)` return what producers wrote, for assertions.

It exercises the same code paths as production — enhancers, error mapping, batch
offsets, transactions, drain — so the behavior you assert in a test is the
behavior you get against a broker.

## Producer Mocks

For a unit test of a service that injects the producer, with no Nest module, use
`createMockKafkaProducer()` — a recording mock `KafkaDriverProducer`:

```ts
import {createMockKafkaProducer, KafkaProducerService} from '@nest-native/kafka';

const {producer, calls} = createMockKafkaProducer();
const service = new KafkaProducerService(producer);
await service.send({topic: 'orders', messages: [{value: 'hi'}]});
expect(calls.send).toHaveLength(1);
```

`createMockTransaction()` provides a recording transaction mock for unit-testing
transactional code paths.

## Driver-Backed Integration Tests

Driver-backed tests run against a real Kafka in CI and are skipped locally when
the `KAFKA_BROKERS` env is missing — the in-memory broker covers the same logic
without native dependencies. See [Quality and CI](quality-and-ci.md) for how the
suite is gated. Secrets and broker credentials never appear in tests, logs, or
docs.
