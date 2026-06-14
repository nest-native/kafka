import { Module } from '@nestjs/common';
import { KafkaModule } from '@nest-native/kafka';
import { OrdersService } from './orders.service';
import { InMemoryBroker } from './in-memory-broker';
import { resolveBrokers, resolveDriverFactory } from './kafka-driver';

/**
 * The in-memory broker is shared so the smoke test can read back exactly what
 * each transaction delivered (or, on abort, did not deliver).
 */
export const broker = new InMemoryBroker();

@Module({
  imports: [
    KafkaModule.forRoot({
      clientId: 'sample-05-transactions',
      client: { brokers: resolveBrokers() },
      // A `transactionalId` turns the shared producer transactional; Confluent's
      // client also enables idempotence automatically. It must be stable and
      // unique per producer instance across the cluster.
      producer: { transactionalId: 'sample-05-orders-producer' },
      driverFactory: resolveDriverFactory(broker),
    }),
  ],
  providers: [OrdersService],
  exports: [OrdersService],
})
export class AppModule {}
