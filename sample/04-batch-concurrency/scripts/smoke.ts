import 'reflect-metadata';
import assert from 'node:assert/strict';
import { Logger } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { AppModule, broker } from '../src/app.module';
import { MetricsConsumer, MetricsSink } from '../src/metrics.consumer';
import { MetricsService } from '../src/metrics.service';
import { resolveBrokers } from '../src/kafka-driver';

async function smoke(): Promise<void> {
  if (resolveBrokers().length > 0) {
    console.log(
      'KAFKA_BROKERS is set; this smoke test exercises the in-memory loopback ' +
        'path, so unset KAFKA_BROKERS to run it.',
    );
    return;
  }

  Logger.overrideLogger(false);
  const app = await NestFactory.createApplicationContext(AppModule, {
    logger: false,
  });
  app.enableShutdownHooks();

  const metrics = app.get(MetricsService);
  const sink = app.get(MetricsSink);
  // Touch the consumer so DI wires it before the first batch.
  app.get(MetricsConsumer);
  sink.reset();
  broker.resolvedOffsets.clear();

  // Ingest a window spanning two partitions: partition 0 gets two metrics,
  // partition 1 gets one. The batch handler runs once per partition batch.
  await metrics.ingest([
    { meter: 'cpu', value: 10, partition: 0 },
    { meter: 'cpu', value: 20, partition: 0 },
    { meter: 'mem', value: 30, partition: 1 },
  ]);

  // 1. Batch consumption: one handler invocation per partition batch, not one
  // per message.
  assert.equal(sink.batches.length, 2, 'one invocation per partition batch');

  const partition0 = sink.batches.find(batch => batch.partition === 0);
  const partition1 = sink.batches.find(batch => batch.partition === 1);
  assert.ok(partition0 && partition1, 'both partitions were aggregated');

  // 2. The whole partition batch is handed to the handler at once.
  assert.equal(partition0.count, 2);
  assert.equal(partition0.sum, 30);
  assert.equal(partition1.count, 1);
  assert.equal(partition1.sum, 30);

  // 3. Rebalance safety (`nestjs/nest#12355`): every message offset is resolved
  // as the batch is processed, so a partition revoked mid-batch keeps its
  // progress instead of replaying the whole batch.
  assert.deepEqual(broker.resolvedOffsets.get('metrics.ingested-0'), [
    '0',
    '1',
  ]);
  assert.deepEqual(broker.resolvedOffsets.get('metrics.ingested-1'), ['0']);

  // 4. Graceful shutdown drains in-flight batches, then disconnects.
  await app.close();

  console.log('Sample 04 batch-concurrency smoke test passed.');
}

void smoke().catch((error: unknown) => {
  console.error(error);
  process.exit(1);
});
