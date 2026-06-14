# Module

`KafkaModule` registers the transport, the producer service, and any consumers.
It mirrors the `forRoot` / `forRootAsync` / `forFeature` shape Nest developers
already know.

## `forRoot`

`forRoot(options)` returns a global `DynamicModule` that provides the resolved
options, the driver, and `KafkaProducerService`:

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

### Options

| Option | Type | Default | Purpose |
| --- | --- | --- | --- |
| `isGlobal` | `boolean` | `true` | Register globally so the producer is available without re-importing. |
| `clientId` | `string` | — | Client id reported to the broker; merged into `client.clientId`. |
| `client` | `KafkaClientConfig` | — | Connection config forwarded to the Confluent client. `brokers` is required to reach a real broker. |
| `producer` | `KafkaProducerConfig` | — | Producer config, including `transactionalId` — see [Transactions](transactions.md). |
| `errorMapper` | `KafkaErrorMapper` | `defaultKafkaErrorMapper` | Map an unhandled handler error to `'commit'` or `'retry'` — see [Error Mapping](error-mapping.md). |
| `concurrency` | `number` | `1` | Default partitions consumed concurrently — see [Batch & Concurrency](batch-and-concurrency.md). |
| `maxInFlight` | `number` | `0` (uncapped) | Default backpressure cap per consumer. |
| `driverFactory` | `KafkaDriverFactory` | Confluent driver | Advanced override; supply a fake driver to test without a broker. |

Credentials (SSL/SASL) belong in `client` and must come from configuration, never
from source. Never log or print them.

## `forRootAsync`

Use `forRootAsync` when the configuration depends on other providers, such as a
`ConfigService`:

```ts
import {Module} from '@nestjs/common';
import {ConfigModule, ConfigService} from '@nestjs/config';
import {KafkaModule} from '@nest-native/kafka';

@Module({
  imports: [
    KafkaModule.forRootAsync({
      imports: [ConfigModule],
      inject: [ConfigService],
      useFactory: (config: ConfigService) => ({
        clientId: config.getOrThrow('KAFKA_CLIENT_ID'),
        client: {brokers: config.getOrThrow<string>('KAFKA_BROKERS').split(',')},
      }),
    }),
  ],
})
export class AppModule {}
```

`forRootAsync` accepts `useFactory` with `inject` and `imports`, the same async
provider contract used across Nest modules.

## `forFeature`

`forFeature([HandlerClass])` returns a non-global module that registers and
exports the supplied consumer classes. Group consumers by feature and import the
feature module where it belongs:

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

A consumer can also be registered directly in a module's `providers` array;
`forFeature` is the convenience for grouping several.

## Next

- [Producer](producer.md): send, sendBatch, and transactional helpers.
- [Consumers](consumers.md): declare handlers and the enhancer pipeline.
