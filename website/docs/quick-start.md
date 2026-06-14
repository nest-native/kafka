# Quick Start

This walkthrough wires a producer and a consumer into a Nest application.

## Install

```bash
npm i @nest-native/kafka @confluentinc/kafka-javascript
```

Install the required Nest peers if your project does not already have them:

```bash
npm i @nestjs/common @nestjs/core @nestjs/microservices reflect-metadata rxjs
```

The published package keeps `"dependencies": {}`. The Confluent client and the
NestJS packages are peers, so applications install only the ecosystems they use.
See the [Support Policy](support-policy.md) for the supported version lines.

## Register The Module

```ts
import {Module} from '@nestjs/common';
import {KafkaModule} from '@nest-native/kafka';

@Module({
  imports: [
    KafkaModule.forRoot({
      clientId: 'orders-service',
      client: {brokers: ['localhost:9092']},
    }),
  ],
})
export class AppModule {}
```

`forRoot` returns a global `DynamicModule`. For configuration resolved from other
providers (a `ConfigService`, for example), use `forRootAsync` — see
[Module](module.md).

## Produce A Message

Inject `KafkaProducerService`. It connects when the module initializes and
disconnects on graceful shutdown:

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

See [Producer](producer.md) for `sendBatch`, `transactional`, and direct producer
access.

## Consume A Message

Mark a class with `@KafkaConsumer` and a method with `@KafkaHandler`. The parsed
payload is the first argument and the raw `KafkaContext` is the second:

```ts
import {Injectable} from '@nestjs/common';
import {KafkaConsumer, KafkaContext, KafkaHandler} from '@nest-native/kafka';

@Injectable()
@KafkaConsumer('orders.placed', {groupId: 'orders-service'})
export class OrdersConsumer {
  @KafkaHandler()
  handle(order: {id: string}, context: KafkaContext): void {
    console.log(`order on ${context.getTopic()}`, order);
  }
}
```

Register the consumer as a provider, either in a module's `providers` or through
`KafkaModule.forFeature([OrdersConsumer])`:

```ts
import {Module} from '@nestjs/common';
import {KafkaModule} from '@nest-native/kafka';
import {OrdersConsumer} from './orders.consumer';
import {OrdersService} from './orders.service';

@Module({
  imports: [KafkaModule.forFeature([OrdersConsumer])],
  providers: [OrdersService],
})
export class OrdersModule {}
```

The payload is JSON-parsed by default, falling back to the decoded string for
non-JSON values. Header conventions stay neutral — the package does not
standardize `traceId` / `correlationId` / `messageType` keys.

## Run It Without A Broker

In tests, swap `KafkaModule` for `KafkaTestModule` to run the whole transport
against an in-memory broker — no `librdkafka`, no `KAFKA_BROKERS`. See
[Testing](testing.md).

## Next Steps

- [Consumers](consumers.md): guards, interceptors, pipes, and filters on handlers.
- [Error Mapping](error-mapping.md): what happens when a handler throws.
- [Batch & Concurrency](batch-and-concurrency.md): process partitions concurrently.
- [Migration Guide](migration.md): port an app off `@nestjs/microservices` Kafka.
