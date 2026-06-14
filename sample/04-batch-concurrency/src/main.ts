import 'reflect-metadata';
import { Logger } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';
import { MetricsService } from './metrics.service';

/**
 * Bootstraps the application context, ingests a window of metrics across two
 * partitions, and lets the batch `@KafkaConsumer` aggregate each partition's
 * batch concurrently. Graceful shutdown drains in-flight batches before
 * disconnect.
 *
 * Run with
 * `npm run start --workspace nest-native-kafka-sample-04-batch-concurrency`.
 */
async function bootstrap(): Promise<void> {
  const app = await NestFactory.createApplicationContext(AppModule);
  app.enableShutdownHooks();

  const metrics = app.get(MetricsService);
  await metrics.ingest([
    { meter: 'cpu', value: 10, partition: 0 },
    { meter: 'cpu', value: 20, partition: 0 },
    { meter: 'mem', value: 30, partition: 1 },
  ]);

  await app.close();
  Logger.log('Sample 04 finished.', 'Bootstrap');
}

void bootstrap();
