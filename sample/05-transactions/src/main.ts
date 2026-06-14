import 'reflect-metadata';
import { Logger } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';
import { OrdersService } from './orders.service';

/**
 * Bootstraps the application context and runs the three transactional flows:
 * a committed multi-topic write, an aborted write (delivers nothing), and a
 * consume-process-produce step that commits offsets atomically. Run with
 * `npm run start --workspace nest-native-kafka-sample-05-transactions`.
 */
async function bootstrap(): Promise<void> {
  const app = await NestFactory.createApplicationContext(AppModule);
  app.enableShutdownHooks();

  const orders = app.get(OrdersService);

  await orders.placeOrder({ id: 'order-1', total: 4200 });

  try {
    await orders.rejectOrder({ id: 'order-2', total: 100 });
  } catch (error) {
    Logger.warn(`rejected as expected: ${(error as Error).message}`, 'Bootstrap');
  }

  await orders.issueReceipt('payment-1', 0, '41');

  await app.close();
  Logger.log('Sample 05 finished.', 'Bootstrap');
}

void bootstrap();
