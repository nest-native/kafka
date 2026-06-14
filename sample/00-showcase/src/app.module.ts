import { Global, Module } from '@nestjs/common';
import { KafkaModule } from '@nest-native/kafka';
import { MessageLog } from './common/message-log.service';
import { InMemoryBroker } from './in-memory-broker';
import { resolveBrokers, resolveDriverFactory } from './kafka-driver';
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
    }),
    OrdersModule,
    NotificationsModule,
  ],
})
export class AppModule {}
