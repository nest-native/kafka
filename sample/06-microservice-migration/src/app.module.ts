import { Module } from '@nestjs/common';
import { KafkaModule } from '@nest-native/kafka';
import { InMemoryKafkaBroker } from '@nest-native/kafka/testing';
import { resolveBrokers, resolveDriverFactory } from './kafka-driver';
import { OrdersConsumer, OrdersInbox } from './orders.consumer';
import { OrdersService } from './orders.service';

/**
 * One in-memory broker shared by producer and consumer so a placed order loops
 * straight back to the ported `@KafkaConsumer` through the full pipeline when no
 * real broker is configured.
 */
const broker = new InMemoryKafkaBroker();

/**
 * The `start` wiring. It mirrors a production setup with `KafkaModule.forRoot`;
 * the smoke test (`scripts/smoke.ts`) instead swaps in `KafkaTestModule` to show
 * the recommended way to test a migrated consumer without a broker.
 */
@Module({
  imports: [
    KafkaModule.forRoot({
      clientId: 'sample-06-microservice-migration',
      client: { brokers: resolveBrokers() },
      driverFactory: resolveDriverFactory(broker),
    }),
  ],
  providers: [OrdersConsumer, OrdersService, OrdersInbox],
  exports: [OrdersService, OrdersInbox],
})
export class AppModule {}
