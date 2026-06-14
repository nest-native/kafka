import 'reflect-metadata';
import { Logger } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';
import { PaymentsService } from './payments.service';

/**
 * Bootstraps the application context, captures a couple of payments, and lets the
 * `@KafkaConsumer` handle them through the milestone-4 parameter decorators and
 * error mapping. Graceful shutdown drains in-flight handlers before disconnect.
 *
 * Run with
 * `npm run start --workspace nest-native-kafka-sample-03-headers-context-errors`.
 */
async function bootstrap(): Promise<void> {
  const app = await NestFactory.createApplicationContext(AppModule);
  app.enableShutdownHooks();

  const payments = app.get(PaymentsService);
  await payments.capture({ id: 'pay-1', amount: 4200 }, 'acme');
  await payments.capture({ id: 'pay-2', amount: -1 }, 'globex'); // 4xx → committed

  await app.close();
  Logger.log('Sample 03 finished.', 'Bootstrap');
}

void bootstrap();
