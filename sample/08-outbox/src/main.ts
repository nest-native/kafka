import 'reflect-metadata';
import { setTimeout as delay } from 'node:timers/promises';
import { Logger } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { type OutboxPublishedEvent, OutboxEvents } from '@nestjs/outbox';
import { filter, firstValueFrom } from 'rxjs';
import { AppModule } from './app.module';
import { BillingLedger } from './billing.consumer';
import { OrdersService } from './orders.service';

/**
 * Bootstraps the application context and places one order. The relay publishes
 * its message through `KafkaOutboxTransport` once the transaction commits, and
 * the billing `@KafkaHandler` charges it.
 *
 * Run with `npm run start --workspace nest-native-kafka-sample-08-outbox`.
 */
async function bootstrap(): Promise<void> {
  const app = await NestFactory.createApplicationContext(AppModule);
  app.enableShutdownHooks();

  const published = firstValueFrom(
    app
      .get(OutboxEvents)
      .events$.pipe(
        filter((event): event is OutboxPublishedEvent => event.type === 'published'),
      ),
  );
  await app.get(OrdersService).place({
    orderId: 'order-1',
    customerId: 'acme-industries',
    total: 150,
  });

  // The relay's published event: the broker acknowledged the record.
  const { message, transport } = await published;
  Logger.log(`Published ${message.topic} ${message.id} through "${transport}"`, 'Bootstrap');

  // In memory the handler has run already. A real broker delivers on its own
  // schedule, and a consumer group seen for the first time starts at the latest
  // offset, so give it a moment rather than an assertion.
  const ledger = app.get(BillingLedger);
  for (let waited = 0; ledger.charges.length === 0 && waited < 5_000; waited += 100) {
    await delay(100);
  }
  Logger.log(`Billing charged ${ledger.charges.length} order(s)`, 'Bootstrap');

  await app.close();
  Logger.log('Sample 08 finished.', 'Bootstrap');
}

void bootstrap();
