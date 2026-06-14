import 'reflect-metadata';
import assert from 'node:assert/strict';
import { NestFactory } from '@nestjs/core';
import { AppModule, broker } from '../src/app.module';
import {
  AUDIT_TOPIC,
  ORDERS_TOPIC,
  OrdersService,
  PAYMENTS_TOPIC,
  RECEIPTS_TOPIC,
} from '../src/orders.service';
import { resolveBrokers } from '../src/kafka-driver';

async function smoke(): Promise<void> {
  if (resolveBrokers().length > 0) {
    console.log(
      'KAFKA_BROKERS is set; the sample would use the real Confluent driver. ' +
        'This smoke test exercises the in-memory transactional path, so unset ' +
        'KAFKA_BROKERS to run it.',
    );
    return;
  }

  const app = await NestFactory.createApplicationContext(AppModule, {
    logger: false,
  });
  app.enableShutdownHooks();

  const orders = app.get(OrdersService);

  // 1. A committed transaction delivers everything it staged, across topics.
  await orders.placeOrder({ id: 'order-1', total: 4200 });
  assert.deepEqual(
    broker.delivered.map(record => record.topic),
    [ORDERS_TOPIC, AUDIT_TOPIC],
  );
  assert.deepEqual(JSON.parse(broker.delivered[0].value), {
    id: 'order-1',
    total: 4200,
  });
  assert.equal(broker.delivered[1].value, 'placed order-1');

  // 2. An aborted transaction delivers nothing and re-throws the original error.
  const deliveredBeforeReject = broker.delivered.length;
  await assert.rejects(
    orders.rejectOrder({ id: 'order-2', total: 100 }),
    /order order-2 rejected/,
  );
  assert.equal(
    broker.delivered.length,
    deliveredBeforeReject,
    'an aborted transaction must deliver nothing',
  );

  // 3. Consume-process-produce: the receipt and the consumer offset commit
  //    atomically via sendOffsets.
  await orders.issueReceipt('payment-1', 0, '41');
  const receipts = broker.delivered.filter(
    record => record.topic === RECEIPTS_TOPIC,
  );
  assert.equal(receipts.length, 1);
  assert.equal(receipts[0].value, 'receipt for payment-1');
  assert.deepEqual(broker.committedOffsets, [
    {
      group: 'payments-worker',
      topics: [
        {
          topic: PAYMENTS_TOPIC,
          // committed offset is consumed offset + 1 ("next offset to read")
          partitions: [{ partition: 0, offset: '42' }],
        },
      ],
    },
  ]);

  await app.close();

  console.log('Sample 05 transactions smoke test passed.');
}

void smoke().catch((error: unknown) => {
  console.error(error);
  process.exit(1);
});
