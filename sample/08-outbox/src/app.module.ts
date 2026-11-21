import { Module } from '@nestjs/common';
import { KafkaModule } from '@nest-native/kafka';
import { InMemoryKafkaBroker } from '@nest-native/kafka/testing';
import { OutboxModule } from '@nestjs/outbox';
import { BillingConsumer, BillingLedger } from './billing.consumer';
import { resolveBrokers, resolveDriverFactory } from './kafka-driver';
import { OrdersService } from './orders.service';
import { SampleOutboxStore } from './sample-outbox.store';
import { PrefixedKafkaOutboxTransport } from './topics';

/**
 * One in-memory broker for the producer and the consumer when no real broker is
 * configured.
 */
const broker = new InMemoryKafkaBroker();

/**
 * The `start` wiring. `KafkaModule` is global, so the transport class that
 * `OutboxModule` instantiates gets its `KafkaProducerService`. The relay runs in
 * this process and polls every second, or right away on `Outbox.notify()`.
 */
@Module({
  imports: [
    KafkaModule.forRoot({
      clientId: 'sample-08-outbox',
      client: { brokers: resolveBrokers() },
      driverFactory: resolveDriverFactory(broker),
    }),
    OutboxModule.forRoot({
      transports: { kafka: PrefixedKafkaOutboxTransport },
    }),
  ],
  providers: [SampleOutboxStore, OrdersService, BillingLedger, BillingConsumer],
})
export class AppModule {}
