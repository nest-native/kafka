import 'reflect-metadata';
import assert from 'node:assert/strict';
import { Logger } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { AppModule } from '../src/app.module';
import { PaymentsInbox } from '../src/payments.consumer';
import { PaymentsService } from '../src/payments.service';
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

  const payments = app.get(PaymentsService);
  const inbox = app.get(PaymentsInbox);
  inbox.reset();

  // 1. A valid payment flows through and the parameter decorators resolve the
  // payload, the `x-tenant` header, and the raw transport context.
  await payments.capture({ id: 'pay-1', amount: 4200 }, 'acme');

  assert.equal(inbox.handled.length, 1);
  assert.deepEqual(inbox.handled[0].payment, { id: 'pay-1', amount: 4200 });
  assert.equal(String(inbox.handled[0].tenant), 'acme');
  assert.equal(inbox.handled[0].topic, 'payments.captured');
  assert.equal(inbox.handled[0].partition, 0);

  // 2. A negative amount throws a BadRequestException (4xx). The default error
  // mapper commits it, so the produce call resolves and the poison message is
  // never redelivered — it simply does not reach the inbox.
  await assert.doesNotReject(
    payments.capture({ id: 'pay-2', amount: -1 }, 'globex'),
  );
  assert.equal(inbox.handled.length, 1, 'the 4xx payment must be committed');

  // 3. Graceful shutdown drains in-flight handlers, then disconnects.
  await app.close();

  console.log('Sample 03 headers-context-errors smoke test passed.');
}

void smoke().catch((error: unknown) => {
  console.error(error);
  process.exit(1);
});
