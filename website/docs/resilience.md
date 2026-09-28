# Resilience and Reconnection

Brokers restart. Nodes get replaced during a rolling upgrade, a leader moves,
a network partition heals. This page states exactly what this package does when
that happens, and points at the test that proves it.

## The short version

**Reconnection is handled by `librdkafka`, in C, underneath the JavaScript
client — not by this package, and not by your application.** When the
connection to a broker drops, the client re-establishes it, refreshes cluster
metadata, re-joins the consumer group, and resumes fetching. Your Nest
application does not restart, your consumers are not re-registered, and
`KafkaProducerService` keeps the same instance.

This is a direct consequence of building on
[`@confluentinc/kafka-javascript`](https://github.com/confluentinc/confluent-kafka-javascript)
rather than a pure-JavaScript client: the connection state machine, backoff,
and metadata refresh are the same battle-tested C implementation the official
Confluent clients use in every other language.

## What is actually proven

Claims about resilience are cheap, so this one is a test rather than a
paragraph. The real-broker integration suite contains a case that:

1. Starts a Nest application with a consumer and the shared producer.
2. Waits until the consumer group is assigned and delivering.
3. Publishes a message and waits for the handler to receive it.
4. **Restarts the broker container out from under the running application**
   (`docker restart`), severing every TCP connection the client holds.
5. Waits for the broker to answer metadata requests again.
6. Publishes a second message and asserts the same handler receives it.

Nothing between steps 3 and 6 restarts the application, re-creates the
producer, or re-subscribes the consumer. If recovery did not happen inside the
client, step 6 would time out.

The test is gated twice, on purpose:

- `KAFKA_BROKERS` must be set (as for the whole integration suite).
- `KAFKA_RESTART_CONTAINER` must name a container the suite is allowed to
  restart. Pointing `KAFKA_BROKERS` at a shared or managed cluster must never
  result in something restarting it, so the capability is named explicitly
  rather than inferred.

Run it locally with:

```bash
npm run infra:up
npm run test:full
npm run infra:down
```

## What recovery does *not* promise

Being honest about the edges matters more than the headline:

- **Sends attempted while every broker is unreachable still fail.** The client
  buffers and retries within its configured limits, but a produce call can
  surface an error during an outage. Treat producing as fallible, as you would
  any network call.
- **Redelivery is still at-least-once.** If the broker restarts between your
  handler completing and its offset being committed, that message is delivered
  again. This is Kafka's delivery model, not a gap in recovery — make handlers
  idempotent.
- **A rebalance can move partitions.** After the group re-joins, a partition
  your instance was processing may be assigned elsewhere. In-flight work is
  drained on shutdown, but assignment is the broker's decision.
- **Recovery is not instant.** Re-connect, metadata refresh, and group re-join
  take time, governed by the client's backoff settings.

## Health Checks

`KafkaHealthIndicator` answers whether the cluster is reachable — for a
readiness probe, or for `@nestjs/terminus`, whose result shape it returns
without the package depending on terminus:

```ts
import {Controller, Get} from '@nestjs/common';
import {HealthCheck, HealthCheckService} from '@nestjs/terminus';
import {KafkaHealthIndicator} from '@nest-native/kafka';

@Controller('health')
export class HealthController {
  constructor(
    private readonly health: HealthCheckService,
    private readonly kafka: KafkaHealthIndicator,
  ) {}

  @Get()
  @HealthCheck()
  check() {
    return this.health.check([() => this.kafka.isHealthy('kafka')]);
  }
}
```

`isHealthy(key?, {timeoutMs?})` resolves to `{kafka: {status: 'up', latencyMs,
topics}}` or `{kafka: {status: 'down', message}}`; a returned `down` is what
fails terminus 11's check with a 503 (verified against `@nestjs/terminus`
11.1.1). The check is a **metadata round trip** (`listTopics`), bounded by
`timeoutMs` (5 s by default) — never `admin.connect()`, which `librdkafka`
performs lazily and which resolves in milliseconds against a stopped broker.
One admin client is reused while checks succeed; after a failure it is
discarded and the next check opens a fresh one, because a client that failed
during an outage keeps reporting it after the cluster is back. Checks that
overlap share one round trip. The real-broker suite freezes the broker under a
running application and requires the indicator to report `down` within the
timeout, then `up` once the broker answers again.

It needs a driver that can open an admin client; the Confluent driver can, and
`KafkaTestModule`'s in-memory broker answers in memory. With a custom driver
that cannot, the indicator reports the cluster `down` rather than guess.

## Tuning

`librdkafka` properties can be set alongside the KafkaJS-style options, in the
same object. Dotted names are recognised automatically; the undotted ones the
client declares (`debug`, `log_level`, the `*_cb` callbacks, `ssl_ca` and
friends) are routed by name, and a test reads the installed client's own type
definitions so that list cannot fall behind a client release:

```ts
KafkaModule.forRoot({
  client: {
    brokers: ['localhost:9092'],
    // librdkafka properties pass straight through
    'reconnect.backoff.ms': 100,
    'reconnect.backoff.max.ms': 10_000,
    'socket.keepalive.enable': true,
    'metadata.max.age.ms': 180_000,
  },
});
```

The defaults are `librdkafka`'s own and are sensible for most deployments;
reach for these when you have measured a reason to.

One name is deliberately *not* routed: `acks` is the single undotted property
the KafkaJS compatibility layer also accepts, so it stays where that layer reads
it. Everything else undotted that the client declares goes to `librdkafka`.

## Observing it

Recovery is silent by design, which makes it hard to see. Consumer and producer
lifecycle events surface through the underlying client, so if you need
visibility during an incident, attach to the raw client with
`@InjectKafkaProducer()` and log its events — the package deliberately does not
impose a logging shape on you.
