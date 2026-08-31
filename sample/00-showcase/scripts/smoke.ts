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

  const first = await orders.placeOrder({
    id: 'order-1',
    tenant: 'acme',
    amount: 4200,
    sku: 'widget',
    quantity: 2,
  });
  await orders.placeOrder({
    id: 'order-2',
    tenant: 'globex',
    amount: 1300,
    sku: 'widget',
    quantity: 1,
  });
  // Missing tenant → inventory still answers, then the guard blocks it.
  await orders.placeOrder({ id: 'order-3', sku: 'widget', quantity: 1 });
  // Out of stock → a successful request whose answer is "no". The order is
  // never published, so nothing downstream of it runs.
  const refused = await orders.placeOrder({
    id: 'order-4',
    tenant: 'acme',
    amount: 900,
    sku: 'gizmo',
    quantity: 1,
  });

  // Request-reply: the publish decision came from a reply, not a guess.
  assert.equal(first, true, 'an in-stock answer must let the order through');
  assert.equal(refused, false, 'an out-of-stock answer must stop the publish');
  assert.equal(
    log.stockChecks.length,
    4,
    'every order asked inventory, including the one that was refused',
  );
  assert.equal(
    log.stockChecks.some(
      check => check.includes('gizmo') && check.includes('unavailable'),
    ),
    true,
    'the replying handler answered the out-of-stock query',
  );
  assert.equal(
    log.handledOrders.includes('order-4'),
    false,
    'a refused order must never reach the orders handler',
  );

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

  // Batch consume + per-topic concurrency (milestone 5): each handled order
  // emits a window of two revenue events on a tenant-keyed partition, and the
  // batch analytics consumer aggregates each window as one batch. Two handled
  // orders → two batches, each holding two events.
  assert.equal(log.batches.length, 2, 'one batch invocation per emitted window');
  assert.deepEqual(
    log.batches.map(batch => batch.count),
    [2, 2],
  );
  // acme → partition 0, globex → partition 1: both partitions were processed.
  assert.deepEqual(
    log.batches.map(batch => batch.partition).sort(),
    [0, 1],
  );

  await app.close();

  console.log('Showcase smoke test passed.');
}

void smoke().catch((error: unknown) => {
  console.error(error);
  process.exit(1);
});
