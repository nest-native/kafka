import 'reflect-metadata';
import assert from 'node:assert/strict';
import { NestFactory } from '@nestjs/core';
import { KafkaProducerService } from '@nest-native/kafka';
import { AppModule } from '../src/app.module';
import { LoggingMessageHandler } from '../src/logging-message.handler';
import { OrdersService } from '../src/orders.service';
import { resolveBrokers } from '../src/kafka-driver';

async function smoke(): Promise<void> {
  if (resolveBrokers().length > 0) {
    console.log(
      'KAFKA_BROKERS is set; the sample would use the real Confluent driver. ' +
        'This smoke test exercises the in-memory loopback path, so unset ' +
        'KAFKA_BROKERS to run it.',
    );
    return;
  }

  const app = await NestFactory.createApplicationContext(AppModule, {
    logger: false,
  });
  app.enableShutdownHooks();

  const producer = app.get(KafkaProducerService);
  const handler = app.get(LoggingMessageHandler);
  const orders = app.get(OrdersService);

  assert.equal(producer.isConnected(), true);

  await orders.placeOrder({ id: 'order-1', total: 4200 });
  await orders.placeOrders([
    { id: 'order-2', total: 1300 },
    { id: 'order-3', total: 9001 },
  ]);

  const received = handler.getReceived();
  assert.equal(received.length, 3);
  assert.deepEqual(
    received.map(entry => entry.topic),
    [OrdersService.topic, OrdersService.topic, OrdersService.topic],
  );
  assert.deepEqual(JSON.parse(received[0].value), { id: 'order-1', total: 4200 });
  assert.deepEqual(JSON.parse(received[2].value), { id: 'order-3', total: 9001 });

  await app.close();
  assert.equal(producer.isConnected(), false);

  console.log('Sample 01 producer-basics smoke test passed.');
}

void smoke().catch((error: unknown) => {
  console.error(error);
  process.exit(1);
});
