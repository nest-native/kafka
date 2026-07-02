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
} from '@nest-native/kafka/testing';

const moduleRef = await Test.createTestingModule({
  imports: [KafkaTestModule.forRoot(), OrdersModule],
}).compile();
await moduleRef.init(); // fires onApplicationBootstrap; consumers subscribe

const broker = moduleRef.get<InMemoryKafkaBroker>(KAFKA_TEST_BROKER);

// Inject a message straight to a consumer...
await broker.emit('orders.placed', {value: JSON.stringify({id: '1'})});
// ...wait for every in-flight handler pipeline to settle (no sleeps)...
await broker.idle();
// ...and assert on what the broker recorded:
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
- `idle()` resolves once every in-flight handler pipeline has settled.
- `getSent()` / `getSentTo(topic)` return what producers wrote, for assertions.

It exercises the same code paths as production — enhancers, error mapping, batch
offsets, transactions, drain — so the behavior you assert in a test is the
behavior you get against a broker.

## Awaiting Handler Completion

Awaiting `emit` (or a producer send) waits for the consumer pipelines that
delivery runs directly. It does **not** wait for work a handler only *starts* —
a fire-and-forget `producer.send(...)` publishing an audit or DLQ record, and
whatever consumer handles *that*. `await broker.idle()` is the settle point for
the whole chain: it keeps waiting until the broker is quiet, following cascades
(a handler produces → another consumer handles → it produces again…) until no
handler pipeline is in flight, and it resolves even when handlers throw (the
error mapping has already decided commit-vs-retry by then):

```ts
await broker.emit('orders.placed', {value: JSON.stringify(order)});
await broker.idle(); // every handler — and every cascade — has settled
expect(broker.getSentTo('orders.audit')).toHaveLength(1); // no sleep needed
```

Prefer `emit` → `idle()` → assert over fixed sleeps: it is exact (no flaky
too-short waits), fast (no padded too-long waits), and it does not stop
consumption — the broker keeps delivering afterwards, so one test can settle and
assert in phases. Work scheduled outside the dispatch chain (a bare
`setTimeout` in a handler) is invisible to the broker and is not awaited.

## Producer Mocks

For a unit test of a service that injects the producer, with no Nest module, use
`createMockKafkaProducer()` — a recording mock `KafkaDriverProducer`:

```ts
import {KafkaProducerService} from '@nest-native/kafka';
import {createMockKafkaProducer} from '@nest-native/kafka/testing';

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
