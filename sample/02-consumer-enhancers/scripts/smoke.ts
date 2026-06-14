import 'reflect-metadata';
import assert from 'node:assert/strict';
import { Logger } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { AppModule } from '../src/app.module';
import { OrdersInbox } from '../src/orders.consumer';
import { OrdersService } from '../src/orders.service';
import { PipelineTrace } from '../src/enhancers';
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

  const orders = app.get(OrdersService);
  const inbox = app.get(OrdersInbox);
  const trace = app.get(PipelineTrace);

  // 1. A valid order flows through guard → interceptor → pipe → handler.
  trace.reset();
  await orders.placeOrder({ id: 'order-1', tenant: 'acme' });

  assert.deepEqual(inbox.handled, [{ id: 'order-1', tenant: 'acme' }]);
  assert.deepEqual(trace.events, [
    'guard',
    'interceptor:before',
    'pipe',
    'handler:order-1',
    'interceptor:after',
  ]);

  // 2. A tenant-less order is blocked by the guard before the handler runs.
  trace.reset();
  await orders.placeOrder({ id: 'order-2' });

  assert.equal(inbox.handled.length, 1, 'guard must block the tenant-less order');
  assert.deepEqual(trace.events, ['guard']);

  // 3. A valid tenant but a payload the pipe rejects is caught by the filter.
  trace.reset();
  await orders.placeOrder({ tenant: 'acme' });

  assert.equal(inbox.handled.length, 1, 'pipe rejection must not reach handler');
  assert.deepEqual(trace.events, [
    'guard',
    'interceptor:before',
    'pipe',
    'filter:order id is required',
  ]);

  await app.close();

  console.log('Sample 02 consumer-enhancers smoke test passed.');
}

void smoke().catch((error: unknown) => {
  console.error(error);
  process.exit(1);
});
