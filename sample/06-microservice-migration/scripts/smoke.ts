import 'reflect-metadata';
import assert from 'node:assert/strict';
import { Module } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { KafkaProducerService } from '@nest-native/kafka';
import {
  InMemoryKafkaBroker,
  KAFKA_TEST_BROKER,
  KafkaTestModule,
} from '@nest-native/kafka/testing';
import {
  OrderPlaced,
  OrdersConsumer,
  OrdersInbox,
} from '../src/orders.consumer';
import { OrdersService } from '../src/orders.service';
import { resolveBrokers } from '../src/kafka-driver';

/**
 * The recommended way to test a migrated consumer: swap `KafkaModule` for
 * `KafkaTestModule`, which runs the whole transport (producer service, the
 * `@KafkaConsumer` pipeline, graceful shutdown) against an in-memory broker. No
 * `@nestjs/testing`, no real Kafka, no native `librdkafka`.
 */
@Module({
  imports: [KafkaTestModule.forRoot({ clientId: 'orders-test' })],
  providers: [OrdersConsumer, OrdersService, OrdersInbox],
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

  const producer = app.get(KafkaProducerService);
  const inbox = app.get(OrdersInbox);
  const broker = app.get<InMemoryKafkaBroker>(KAFKA_TEST_BROKER);

  assert.equal(producer.isConnected(), true);

  // 1. Producing through the migrated service reaches the ported consumer.
  //    broker.idle() is the settle point: it resolves once every in-flight
  //    handler pipeline (and anything a handler produced in turn) has finished,
  //    so the assertions below never race an async handler — no sleeps.
  await app.get(OrdersService).placeOrder({ id: 'order-1', total: 4200 });
  await broker.idle();
  assert.equal(inbox.handled.length, 1);
  assert.deepEqual(inbox.handled[0].order, { id: 'order-1', total: 4200 });
  assert.equal(inbox.handled[0].topic, OrdersService.topic);

  // 2. The broker records what was produced, so a test can assert on it.
  assert.deepEqual(broker.getSentTo(OrdersService.topic), [
    {
      key: 'order-1',
      value: JSON.stringify({ id: 'order-1', total: 4200 }),
    },
  ]);

  // 3. emit() injects a message straight to the consumer, no producer needed;
  //    idle() again guarantees the handler pipeline has settled before asserting.
  const injected: OrderPlaced = { id: 'order-2', total: 1300 };
  await broker.emit(OrdersService.topic, { value: JSON.stringify(injected) });
  await broker.idle();
  assert.equal(inbox.handled.length, 2);
  assert.deepEqual(inbox.handled[1].order, injected);

  await app.close();
  assert.equal(producer.isConnected(), false);

  console.log('Sample 06 microservice-migration smoke test passed.');
}

void smoke().catch((error: unknown) => {
  console.error(error);
  process.exit(1);
});
