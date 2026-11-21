import 'reflect-metadata';
import assert from 'node:assert/strict';
import { Module } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import {
  InMemoryKafkaBroker,
  KAFKA_TEST_BROKER,
  KafkaTestModule,
} from '@nest-native/kafka/testing';
import { OutboxModule, OutboxRelay } from '@nestjs/outbox';
import { BillingConsumer, BillingLedger } from '../src/billing.consumer';
import { resolveBrokers } from '../src/kafka-driver';
import { OrdersService } from '../src/orders.service';
import { SampleOutboxStore } from '../src/sample-outbox.store';
import {
  ORDERS_PLACED,
  PrefixedKafkaOutboxTransport,
  TOPIC_PREFIX,
} from '../src/topics';

/**
 * The outbox flow end to end, without a broker: `KafkaTestModule` in place of
 * `KafkaModule`, and the relay driven with `runOnce()` instead of its poll loop,
 * so every step is deterministic. No `@nestjs/testing`, no real Kafka, no native
 * `librdkafka`.
 *
 * What an in-memory broker cannot show — the relay resolving on a real
 * acknowledgement, the record matching the official `ClientProxyTransport`'s
 * byte for byte through a real `ClientKafka`, and the broker's permanent
 * refusals dead-lettered on the first attempt — lives in the package's
 * `KAFKA_BROKERS`-gated integration suite.
 */
@Module({
  imports: [
    KafkaTestModule.forRoot({ clientId: 'sample-08-test' }),
    OutboxModule.forRoot({
      transports: { kafka: PrefixedKafkaOutboxTransport },
      relay: { enabled: false },
    }),
  ],
  providers: [SampleOutboxStore, OrdersService, BillingLedger, BillingConsumer],
})
class TestAppModule {}

async function smoke(): Promise<void> {
  if (resolveBrokers().length > 0) {
    console.log(
      'KAFKA_BROKERS is set; this smoke test exercises the in-memory ' +
        'KafkaTestModule path. Unset KAFKA_BROKERS to run it.',
    );
    return;
  }

  const app = await NestFactory.createApplicationContext(TestAppModule, {
    logger: false,
  });
  app.enableShutdownHooks();

  const orders = app.get(OrdersService);
  const ledger = app.get(BillingLedger);
  const relay = app.get(OutboxRelay);
  const broker = app.get<InMemoryKafkaBroker>(KAFKA_TEST_BROKER);
  const topic = `${TOPIC_PREFIX}${ORDERS_PLACED}`;
  const order = { orderId: 'order-1', customerId: 'acme-industries', total: 150 };

  // 1. Placing the order writes it and its message in one transaction. Nothing
  //    reaches Kafka until the relay publishes the message.
  const id = await orders.place(order);
  assert.equal(orders.orders.has('order-1'), true);
  assert.equal(broker.getSentTo(topic).length, 0);

  // 2. The relay publishes it through KafkaOutboxTransport. Publishing resolves
  //    on the broker's acknowledgement, and only then is the message marked
  //    published.
  const published = await relay.runOnce();
  assert.equal(published.published, 1);
  await broker.idle();

  // 3. On the wire: the prefixed topic, the order id as the key, the message's
  //    headers, and the outbox envelope as the value, which is the record the
  //    official ClientProxyTransport sends through a ClientKafka.
  const records = broker.getSentTo(topic);
  assert.equal(records.length, 1);
  const [record] = records;
  assert.equal(record.key, 'order-1');
  assert.deepEqual(record.headers, { 'x-customer-id': 'acme-industries' });
  const envelope = JSON.parse(String(record.value));
  assert.equal(envelope.id, id);
  assert.equal(envelope.topic, ORDERS_PLACED);
  assert.deepEqual(envelope.payload, order);
  assert.deepEqual(ledger.charges, [order]);

  // 4. Kafka delivers the same record again: OutboxInbox finds its envelope id
  //    recorded, and billing does not charge twice.
  await broker.emit(topic, record);
  await broker.idle();
  assert.deepEqual(ledger.charges, [order]);

  // 5. A transaction that fails takes the order and its message with it: the
  //    relay finds nothing to publish.
  await assert.rejects(
    orders.place(
      { orderId: 'order-2', customerId: 'acme-industries', total: 99 },
      () => {
        throw new Error('payment declined');
      },
    ),
    /payment declined/,
  );
  assert.equal(orders.orders.has('order-2'), false);
  assert.equal((await relay.runOnce()).claimed, 0);
  assert.equal(broker.getSentTo(topic).length, 2);

  await app.close();
  console.log('Sample 08 outbox smoke test passed.');
}

void smoke().catch((error: unknown) => {
  console.error(error);
  process.exit(1);
});
