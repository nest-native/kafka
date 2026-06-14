import 'reflect-metadata';
import { Logger } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';
import { OrdersService } from './orders.service';

/**
 * Bootstraps the application context and places an order, which the ported
 * `@KafkaConsumer` handles through the full Nest enhancer pipeline. Graceful
 * shutdown drains in-flight handlers before disconnect.
 *
 * Run with
 * `npm run start --workspace nest-native-kafka-sample-06-microservice-migration`.
 */
async function bootstrap(): Promise<void> {
  const app = await NestFactory.createApplicationContext(AppModule);
  app.enableShutdownHooks();

  const orders = app.get(OrdersService);
  await orders.placeOrder({ id: 'order-1', total: 4200 });

  await app.close();
  Logger.log('Sample 06 finished.', 'Bootstrap');
}

void bootstrap();
