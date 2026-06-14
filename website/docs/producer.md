# Producer

`KafkaProducerService` is the injectable producer. It connects when the module
initializes and disconnects on graceful shutdown. For low-level access to the raw
Confluent producer, inject it directly with `@InjectKafkaProducer()`.

## Send A Message

```ts
import {Injectable} from '@nestjs/common';
import {KafkaProducerService} from '@nest-native/kafka';

@Injectable()
export class OrdersService {
  constructor(private readonly producer: KafkaProducerService) {}

  async placeOrder(id: string): Promise<void> {
    await this.producer.send({
      topic: 'orders.placed',
      messages: [{key: id, value: JSON.stringify({id})}],
    });
  }
}
```

Serialization is explicit: pass a `string` or `Buffer` `value`. The package does
not impose a serializer or standardize header keys.

## Send To Many Topics

`sendBatch` writes to several topics in one call:

```ts
await this.producer.sendBatch({
  topicMessages: [
    {topic: 'orders.placed', messages: [{value: id}]},
    {topic: 'audit.log', messages: [{value: `order ${id}`}]},
  ],
});
```

## Transactional Send

`transactional(work)` runs `work` inside one Kafka transaction: it commits when
`work` resolves and aborts — delivering nothing — when it throws, re-raising the
original error.

```ts
await this.producer.transactional(async tx => {
  await tx.send({topic: 'orders.placed', messages: [{value: id}]});
  await tx.sendBatch({
    topicMessages: [{topic: 'orders.audit', messages: [{value: `placed ${id}`}]}],
  });
});
```

Transactions require a `transactionalId` on the producer config. See
[Transactions](transactions.md) for the full helper, the read-process-write
`sendOffsets` pattern, and the abort-error semantics.

## Direct Producer Access

When you need the raw Confluent producer — for an API the service does not wrap —
inject it with `@InjectKafkaProducer()`:

```ts
import {Injectable} from '@nestjs/common';
import {InjectKafkaProducer, KafkaDriverProducer} from '@nest-native/kafka';

@Injectable()
export class AdvancedProducer {
  constructor(@InjectKafkaProducer() private readonly producer: KafkaDriverProducer) {}
}
```

Reaching for the raw producer is an opt-in escape hatch; prefer
`KafkaProducerService` so connection lifecycle and transactions stay managed.

## Testing The Producer

For a unit test of a service that injects the producer, with no Nest module, use
`createMockKafkaProducer()` — a recording mock. See [Testing](testing.md).
