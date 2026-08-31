import 'reflect-metadata';
import { Logger } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';
import { OrdersService } from './orders/orders.service';

/**
 * Bootstraps the showcase, places a few orders, and lets the consumer pipeline
 * and the chained notification consumer process them. Run with
 * `npm run start --workspace nest-native-kafka-showcase`.
 *
 * Each order first asks inventory — over request-reply — whether it can be
 * fulfilled, so the run demonstrates all three outcomes a caller has to handle:
 * an answer that says yes, an answer that says no, and no answer at all.
 */
async function bootstrap(): Promise<void> {
  const app = await NestFactory.createApplicationContext(AppModule);
  app.enableShutdownHooks();

  const orders = app.get(OrdersService);

  // In stock: the reply says available, so the order is published and the whole
  // event pipeline downstream of it runs.
  await orders.placeOrder({
    id: 'order-1',
    tenant: 'acme',
    amount: 4200,
    sku: 'widget',
    quantity: 2,
  });

  // Out of stock: a perfectly successful request whose *answer* is no. A reply
  // is not the same thing as a success, and the caller decides what to do.
  await orders.placeOrder({
    id: 'order-2',
    tenant: 'globex',
    amount: 1300,
    sku: 'gizmo',
    quantity: 1,
  });

  // Missing tenant: blocked by the guard downstream, after inventory answers.
  await orders.placeOrder({
    id: 'order-3',
    sku: 'widget',
    quantity: 1,
  });

  await app.close();
  Logger.log('Showcase finished.', 'Bootstrap');
}

void bootstrap();
