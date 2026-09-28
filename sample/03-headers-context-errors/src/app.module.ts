import { Module } from '@nestjs/common';
import { KafkaModule } from '@nest-native/kafka';
import { InMemoryBroker } from './in-memory-broker';
import { resolveBrokers, resolveDriverFactory } from './kafka-driver';
import { DeadLetterFilter } from './dead-letter.filter';
import {
  PaymentsConsumer,
  PaymentsDeadLetters,
  PaymentsInbox,
} from './payments.consumer';
import { PaymentsService } from './payments.service';

/**
 * One in-memory broker shared by producer and consumer so a captured payment
 * loops straight back to the `@KafkaConsumer` through the full pipeline.
 */
const broker = new InMemoryBroker();

@Module({
  imports: [
    KafkaModule.forRoot({
      clientId: 'sample-03-headers-context-errors',
      client: { brokers: resolveBrokers() },
      driverFactory: resolveDriverFactory(broker),
      // The default error mapper commits 4xx client errors and retries
      // everything else. The payments handler dead-letters its 4xx failures
      // through DeadLetterFilter before they are committed.
    }),
  ],
  providers: [
    PaymentsConsumer,
    PaymentsDeadLetters,
    DeadLetterFilter,
    PaymentsService,
    PaymentsInbox,
  ],
  exports: [PaymentsService, PaymentsInbox],
})
export class AppModule {}
