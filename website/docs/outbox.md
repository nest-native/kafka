# Transactional Outbox

`@nest-native/kafka/outbox` ships `KafkaOutboxTransport`, a transport for the
official [`@nestjs/outbox`](https://www.npmjs.com/package/@nestjs/outbox) relay
that publishes through this package's `KafkaProducerService`, on Confluent's
client. The outbox writes a message in the same database transaction as your
business data, and its relay publishes it after the commit, with leases, retries
and a dead-letter table. This page covers the Kafka end: wiring the transport,
the record it produces, how it fails, and how a `@KafkaHandler` consumes it.

The official route to Kafka is `ClientProxyTransport` over a `ClientKafka`,
which runs on `kafkajs`. `KafkaOutboxTransport` produces the same record, byte
for byte, through the producer your application already has.

## Install

```bash
npm i @nest-native/kafka @confluentinc/kafka-javascript @nestjs/outbox
```

`@nestjs/outbox` is an **optional** peer (`^0.1.0`), imported only by the
`@nest-native/kafka/outbox` entrypoint, so applications that do not use it never
load it. It is ESM-only: this package, like any CommonJS application, loads it
through Node's `require(esm)`, which needs Node.js `>=22.12` — the same floor as
NestJS 12.

## Wiring

`KafkaModule` is global by default, so the transport finds the shared
`KafkaProducerService` wherever `OutboxModule` builds it. Register it by name,
in one of three forms:

```ts
import {KafkaProducerService} from '@nest-native/kafka';
import {KafkaOutboxTransport} from '@nest-native/kafka/outbox';
import {OutboxModule} from '@nestjs/outbox';

// The class: Nest instantiates it with the producer.
OutboxModule.forRoot({transports: {kafka: KafkaOutboxTransport}});

// The class with options: with() returns a configured subclass.
OutboxModule.forRoot({
  transports: {
    kafka: KafkaOutboxTransport.with({
      toRecord: message => ({topic: `${prefix}${message.topic}`}),
    }),
  },
});

// An instance, when the options come from injected configuration.
OutboxModule.forRootAsync({
  inject: [KafkaProducerService, ConfigService],
  useFactory: (producer: KafkaProducerService, config: ConfigService) => ({
    transports: {
      kafka: new KafkaOutboxTransport(producer, {
        toRecord: message => ({
          topic: `${config.getOrThrow('TOPIC_PREFIX')}${message.topic}`,
        }),
      }),
    },
  }),
});
```

With a single transport and no `@OnOutboxMessage()` handlers, the relay sends
every message to it; with several destinations, `OutboxModule`'s `route` picks
one per message. With `KafkaModule.forRoot({isGlobal: false})`, pass that same
module to `OutboxModule.forRoot({imports: [...]})`.

Producing is the outbox's own API: `Outbox.add(tx, message)` inside your
transaction, and `outbox.notify()` after the commit to publish without waiting
for the next poll. The message's `key` is the ordering key — the relay publishes
one key's messages one at a time, in order — and becomes the record key, so they
land on one partition.

## The Record on the Wire

By default the transport produces the record the official `ClientProxyTransport`
produces through a `ClientKafka` (its `toKafkaRecord`, since `@nestjs/outbox`
0.1.3), so a consumer written for that path keeps working when an application
swaps transports:

| Record | Value |
| --- | --- |
| topic | the message's `topic` |
| key | the message's `key`, `null` when it has none |
| headers | the message's `headers` |
| value | the JSON of the message's `OutboxEnvelope` |
| partition | none: the key picks it |

```json
{"id":"0192f5c4-…","topic":"orders.placed","key":"order-1","headers":{"x-tenant":"acme"},"createdAt":1763700000000,"payload":{"orderId":"order-1","total":42}}
```

The envelope's fields are in that order, so the JSON is the same bytes either
transport sends. A unit test compares the record with what the installed
`ClientProxyTransport` hands Nest's `KafkaRequestSerializer`, and the real-broker
suite consumes both from a broker and compares the key, value and headers byte
for byte.

## Changing the Wire: `toRecord`

`toRecord(message, envelope)` returns what differs from the default record:
`topic`, `key`, `value`, `headers` and `partition`, named as in the producer's own
types. A field left out (or `undefined`) keeps the default, so returning
`{topic}` alone only renames the topic; `null` is kept (a `null` key, a `null`
value). `headers` replaces the default headers as a whole: spread
`message.headers` into it to keep them.

A topic prefix per environment:

```ts
KafkaOutboxTransport.with({
  toRecord: message => ({topic: `${prefix}${message.topic}`}),
});
```

A wire of your own — the bare payload as the value, the business key as the
record key, and id headers a consumer can deduplicate by:

```ts
KafkaOutboxTransport.with({
  toRecord: message => {
    const idempotencyKey = message.headers['x-idempotency-key'] ?? message.id;
    return {
      topic: `${prefix}${message.topic}`,
      key: idempotencyKey,
      value: JSON.stringify(message.payload),
      headers: {'x-event-id': message.id, 'x-idempotency-key': idempotencyKey},
    };
  },
});
```

`toRecord` is called with no `this`. Throw `NonRetryableMessageError` from it to
dead-letter a message it cannot map; anything else it throws fails the attempt,
which the relay retries.

## Failures

A publish resolves once the broker acknowledged the record, under the producer's
`acks` (all in-sync replicas by default). A failure a retry can never fix is
thrown as `NonRetryableMessageError`, with the client's error as its `cause`, and
the relay dead-letters the message on that attempt. The transport reads the
numeric `code` Confluent's client puts on both of its error shapes (librdkafka's
codes; for a broker error, the Kafka protocol's):

| Code | librdkafka name | When |
| --- | --- | --- |
| `10` | `ERR_MSG_SIZE_TOO_LARGE` | The record is over the producer's `message.max.bytes` (refused before sending) or the topic's `max.message.bytes` (refused by the broker). |
| `17` | `ERR_TOPIC_EXCEPTION` | The topic name is not a legal one. |
| `29` | `ERR_TOPIC_AUTHORIZATION_FAILED` | The client's principal may not write to the topic. |
| `87` | `ERR_INVALID_RECORD` | The broker failed to validate the record, such as a record without a key on a compacted topic. |

Everything else is thrown as it is, and the relay retries it with backoff — a
timeout, an unreachable broker, a topic that does not exist yet, a full local
queue, a fatal producer error, and two failures that look permanent but are not
about this record: `ERR__INVALID_DIFFERENT_RECORD` (another record in the same
batch was invalid) and `ERR_RECORD_LIST_TOO_LARGE` (the batch was over the
segment size).

librdkafka batches records, and when the broker refuses a batch as too large,
every record in it fails with that code, small ones included. Keep the
producer's `batch.size` at or below the smallest `max.message.bytes` of the
topics the outbox publishes to, so that a batch is only ever too large because
one record is.

A dead letter can be requeued once its cause is fixed — an ACL granted, a topic
limit raised — with `OutboxDeadLetters.requeue()`. It keeps its id, so a
consumer's inbox still recognizes it.

## Delivery Is At-Least-Once

A produce cannot be called back. The transport checks the relay's `signal`
before producing, but once a record is handed to the client, it is delivered or
fails on the client's own schedule. A send that outlives `relay.publishTimeout`
counts as a failed attempt, and may still land next to its retry; a relay that
crashes between the acknowledgement and marking the message published sends it
again. Consumers deduplicate by the envelope's `id`.

To keep the window small, set the producer's `message.timeout.ms` (librdkafka's
delivery timeout, 300 s by default) below `relay.publishTimeout` (a third of the
lease, 10 s by default), so a send gives up before the relay stops waiting for
it. The producer is shared, so the timeout applies to every send the
application makes:

```ts
KafkaModule.forRoot({
  client: {brokers: ['localhost:9092']},
  producer: {'message.timeout.ms': 8_000},
});
```

## Consuming: Read the Envelope, Deduplicate

No extra API is needed on the consuming side. The record's value is the
envelope's JSON, so `@KafkaMessage()` hands a handler
`{id, topic, key, headers, createdAt, payload}`, and `OutboxInbox` (from
`OutboxModule`) runs the work once per id:

```ts
import {Injectable} from '@nestjs/common';
import {KafkaConsumer, KafkaHandler, KafkaMessage} from '@nest-native/kafka';
import {type OutboxEnvelope, OutboxInbox} from '@nestjs/outbox';

@Injectable()
@KafkaConsumer('orders.placed', {groupId: 'billing'})
export class BillingConsumer {
  constructor(
    private readonly inbox: OutboxInbox,
    private readonly billing: BillingService,
  ) {}

  @KafkaHandler()
  async onOrderPlaced(@KafkaMessage() envelope: OutboxEnvelope<OrderPlaced>) {
    await this.inbox.process('billing', envelope.id, () =>
      this.billing.charge(envelope.payload),
    );
  }
}
```

`process()` records the id after the work succeeds, so a redelivery is skipped
and a failed attempt runs again — throw, and the error mapper's `'retry'` has
Kafka redeliver it. Name the consumer per consumer group, so two groups reading
the same topic keep separate records. Across instances the inbox needs a shared
store (PostgreSQL, MySQL), and when the side effects live in that database,
`processInTransaction(tx, ...)` makes them exactly-once. Validate the envelope
the way you validate any payload — a pipe on `@KafkaMessage()` — and with a
custom wire, deduplicate by whatever id it carries, such as an `x-event-id`
header read with `@KafkaHeaders()`.

## Testing

`KafkaTestModule` runs the whole flow in memory. Disable the relay's poll loop
and drive it with `runOnce()`:

```ts
const moduleRef = await Test.createTestingModule({
  imports: [
    KafkaTestModule.forRoot(),
    OutboxModule.forRoot({
      transports: {kafka: KafkaOutboxTransport},
      relay: {enabled: false},
    }),
    BillingModule,
  ],
}).compile();
await moduleRef.init();

await placeOrder(moduleRef); // Outbox.add(tx, ...) in a transaction
await moduleRef.get(OutboxRelay).runOnce(); // publishes, and the handler runs
await moduleRef.get<InMemoryKafkaBroker>(KAFKA_TEST_BROKER).idle();
```

Without a registered store, the outbox runs on its in-memory default
(`moduleRef.get(OutboxStorage).messages`), whose `transaction(work)` gives
`Outbox.add()` a handle that commits when `work` resolves and rolls back when it
throws.

[Sample 08](samples/catalog.md#08-outbox) runs this flow — the transaction, the
relay, a redelivery that is skipped, and a rollback that publishes nothing.
