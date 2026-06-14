import 'reflect-metadata';
import { Logger } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';
import { OrdersService } from './orders.service';

/**
 * Bootstraps the application context, publishes a couple of orders, and lets the
 * `@KafkaConsumer` handle them through the full enhancer pipeline. Run with
 * `npm run start --workspace nest-native-kafka-sample-02-consumer-enhancers`.
 */
async function bootstrap(): Promise<void> {
  const app = await NestFactory.createApplicationContext(AppModule);
  app.enableShutdownHooks();

  const orders = app.get(OrdersService);
  await orders.placeOrder({ id: 'order-1', tenant: 'acme' });
  await orders.placeOrder({ id: 'order-2' }); // missing tenant → blocked by guard

  await app.close();
  Logger.log('Sample 02 finished.', 'Bootstrap');
}

void bootstrap();
