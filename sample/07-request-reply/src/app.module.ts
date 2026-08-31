import { Module } from '@nestjs/common';
import { KafkaModule } from '@nest-native/kafka';
import { InMemoryKafkaBroker } from '@nest-native/kafka/testing';
import { resolveBrokers, resolveDriverFactory } from './kafka-driver';
import { TotalsClient } from './totals.client';
import { TotalsConsumer, TotalsInbox } from './totals.consumer';

/**
 * One in-memory broker shared by producer, reply consumer, and handler consumer
 * so a request loops all the way back to its caller through the full pipeline
 * when no real broker is configured.
 */
const broker = new InMemoryKafkaBroker();

/** The shared reply topic. Provisioned by you in production, like any topic. */
export const REPLY_TOPIC = 'sample-07.replies';

/**
 * The `start` wiring, mirroring a production setup.
 *
 * `requestReply` is the entire client-side opt-in: without it no reply consumer
 * is created, no topic is touched, and `request()` rejects with an error naming
 * the missing option. The replying handlers in `TotalsConsumer` need nothing
 * from this block — they answer wherever the request's headers point.
 */
@Module({
  imports: [
    KafkaModule.forRoot({
      clientId: 'sample-07-request-reply',
      client: { brokers: resolveBrokers() },
      driverFactory: resolveDriverFactory(broker),
      requestReply: {
        replyTopic: REPLY_TOPIC,
        // A short budget keeps the sample snappy; the default is 30s.
        timeoutMs: 5_000,
      },
    }),
  ],
  providers: [TotalsConsumer, TotalsInbox, TotalsClient],
  exports: [TotalsClient, TotalsInbox],
})
export class AppModule {}
