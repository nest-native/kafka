import 'reflect-metadata';
import assert from 'node:assert/strict';
import { Logger } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { AppModule } from '../src/app.module';
import { MessageLog } from '../src/common/message-log.service';
import { OrdersService } from '../src/orders/orders.service';
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
  const log = app.get(MessageLog);
  log.reset();

  await orders.placeOrder({ id: 'order-1', tenant: 'acme', amount: 4200 });
  await orders.placeOrder({ id: 'order-2', tenant: 'globex', amount: 1300 });
  await orders.placeOrder({ id: 'order-3' }); // missing tenant → guard blocks

  // Two valid orders reached the handler; the tenant-less one was blocked.
  assert.deepEqual(log.handledOrders, ['order-1', 'order-2']);

  // Each handled order produced a chained notification consumed by the second
  // feature module.
  assert.deepEqual(log.notifications, [
    'Order order-1 confirmed',
    'Order order-2 confirmed',
  ]);

  // Request scoping: each handled order got its own audit instance.
  assert.equal(log.auditedBy.length, 2);
  assert.equal(new Set(log.auditedBy).size, 2);

  // The blocked order ran the guard only; no handler, no notification for it.
  assert.equal(
    log.pipeline.filter(event => event === 'guard').length >= 3,
    true,
  );

  await app.close();

  console.log('Showcase smoke test passed.');
}

void smoke().catch((error: unknown) => {
  console.error(error);
  process.exit(1);
});
