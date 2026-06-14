import 'reflect-metadata';
import { Logger } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';
import { OrdersService } from './orders/orders.service';

/**
 * Bootstraps the showcase, places a few orders, and lets the consumer pipeline
 * and the chained notification consumer process them. Run with
 * `npm run start --workspace nest-native-kafka-showcase`.
 */
async function bootstrap(): Promise<void> {
  const app = await NestFactory.createApplicationContext(AppModule);
  app.enableShutdownHooks();

  const orders = app.get(OrdersService);
  await orders.placeOrder({ id: 'order-1', tenant: 'acme', amount: 4200 });
  await orders.placeOrder({ id: 'order-2', tenant: 'globex', amount: 1300 });
  await orders.placeOrder({ id: 'order-3' }); // missing tenant → blocked

  await app.close();
  Logger.log('Showcase finished.', 'Bootstrap');
}

void bootstrap();
