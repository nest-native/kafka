import 'reflect-metadata';
import { Logger } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';
import { TotalsClient } from './totals.client';

/**
 * Bootstraps the application context and issues one request, which the
 * `reply: true` handler answers through the full Nest enhancer pipeline.
 *
 * Run with
 * `npm run start --workspace nest-native-kafka-sample-07-request-reply`.
 */
async function bootstrap(): Promise<void> {
  const app = await NestFactory.createApplicationContext(AppModule);
  app.enableShutdownHooks();

  const result = await app.get(TotalsClient).total('acme-industries');
  Logger.log(`Total for ${result.customerId}: ${result.total}`, 'Bootstrap');

  await app.close();
  Logger.log('Sample 07 finished.', 'Bootstrap');
}

void bootstrap();
