import { Global, Module } from '@nestjs/common';
import { KafkaModule } from '@nest-native/kafka';
import { MessageLog } from './common/message-log.service';
import { InMemoryBroker } from './in-memory-broker';
import { resolveBrokers, resolveDriverFactory } from './kafka-driver';
import { AnalyticsModule } from './analytics/analytics.module';
import { NotificationsModule } from './notifications/notifications.module';
import { OrdersModule } from './orders/orders.module';

/**
 * The in-memory broker is shared by every producer and consumer in the showcase
 * so produced messages loop back through the real `@KafkaConsumer` pipeline.
 */
const broker = new InMemoryBroker();

/**
 * Provides the singleton {@link MessageLog} to every feature module.
 */
@Global()
@Module({
  providers: [MessageLog],
  exports: [MessageLog],
})
class SharedModule {}

@Module({
  imports: [
    SharedModule,
    KafkaModule.forRoot({
      clientId: 'sample-00-showcase',
      client: { brokers: resolveBrokers() },
      driverFactory: resolveDriverFactory(broker),
      // Backpressure: cap how many messages/batches any one consumer processes
      // at once. A `@KafkaConsumer` or `@KafkaHandler` may raise or lower it.
      maxInFlight: 16,
    }),
    OrdersModule,
    NotificationsModule,
    AnalyticsModule,
  ],
})
export class AppModule {}
