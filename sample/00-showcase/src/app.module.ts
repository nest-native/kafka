import { Global, Module } from '@nestjs/common';
import { KafkaModule } from '@nest-native/kafka';
import { MessageLog } from './common/message-log.service';
import { InMemoryBroker } from './in-memory-broker';
import { resolveBrokers, resolveDriverFactory } from './kafka-driver';
import { AnalyticsModule } from './analytics/analytics.module';
import { NotificationsModule } from './notifications/notifications.module';
import { InventoryModule } from './inventory/inventory.module';
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
      // A `transactionalId` makes the shared producer transactional so
      // `OrdersService.placeOrder` can publish through the transactional helper.
      producer: { transactionalId: 'sample-00-showcase-producer' },
      // Backpressure: cap how many messages/batches any one consumer processes
      // at once. A `@KafkaConsumer` or `@KafkaHandler` may raise or lower it.
      maxInFlight: 16,
      // Opt into request-reply. Without this block the feature is inert: no
      // reply consumer is created and nothing is paid for by applications that
      // only publish events. The reply topic is infrastructure you provision,
      // like every other topic this package consumes.
      requestReply: {
        replyTopic: 'showcase.replies',
        // The showcase must finish promptly even when nothing answers, so the
        // deliberate no-listener demo below does not stall it for the 30s
        // default.
        timeoutMs: 2_000,
      },
    }),
    OrdersModule,
    InventoryModule,
    NotificationsModule,
    AnalyticsModule,
  ],
})
export class AppModule {}
