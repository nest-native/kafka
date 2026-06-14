import { Module } from '@nestjs/common';
import { KafkaModule } from '@nest-native/kafka';
import { LoggingMessageHandler } from './logging-message.handler';
import { OrdersService } from './orders.service';
import { resolveBrokers, resolveDriverFactory } from './kafka-driver';

/**
 * Shared logging handler instance. The in-memory driver delivers messages to it
 * and the smoke test reads back what it received, so it must be the same object
 * the driver factory closes over and the one Nest injects.
 */
const loggingHandler = new LoggingMessageHandler();

@Module({
  imports: [
    KafkaModule.forRoot({
      clientId: 'sample-01-producer-basics',
      client: { brokers: resolveBrokers() },
      driverFactory: resolveDriverFactory(loggingHandler),
    }),
  ],
  providers: [
    OrdersService,
    { provide: LoggingMessageHandler, useValue: loggingHandler },
  ],
  exports: [OrdersService, LoggingMessageHandler],
})
export class AppModule {}
