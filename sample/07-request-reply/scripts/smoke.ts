import 'reflect-metadata';
import assert from 'node:assert/strict';
import { Module } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import {
  KafkaReplyRemoteError,
  KafkaReplyTimeoutError,
} from '@nest-native/kafka';
import {
  InMemoryKafkaBroker,
  KAFKA_TEST_BROKER,
  KafkaTestModule,
} from '@nest-native/kafka/testing';
import { REPLY_TOPIC } from '../src/app.module';
import { resolveBrokers } from '../src/kafka-driver';
import { TotalsClient } from '../src/totals.client';
import { TotalsConsumer, TotalsInbox } from '../src/totals.consumer';

/**
 * The recommended way to test request-reply: swap `KafkaModule` for
 * `KafkaTestModule`, which runs the whole thing — the reply consumer, the
 * correlation map, the readiness handshake, timeouts, and error replies —
 * against an in-memory broker. No `@nestjs/testing`, no real Kafka, no native
 * `librdkafka`.
 *
 * The real-broker properties this cannot show — group coordination for the
 * per-instance reply groups, reply affinity across three concurrent instances,
 * a request spanning a broker restart, and byte-level interop with a real
 * `@nestjs/microservices` `ServerKafka` / `ClientKafka` — live in the package's
 * `KAFKA_BROKERS`-gated integration suite.
 */
@Module({
  imports: [
    KafkaTestModule.forRoot({
      clientId: 'sample-07-test',
      requestReply: { replyTopic: REPLY_TOPIC, timeoutMs: 5_000 },
    }),
  ],
  providers: [TotalsConsumer, TotalsInbox, TotalsClient],
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

  const client = app.get(TotalsClient);
  const inbox = app.get(TotalsInbox);
  const broker = app.get<InMemoryKafkaBroker>(KAFKA_TEST_BROKER);

  // 1. The round trip: request() produces to the request topic, the
  //    `reply: true` handler's return value comes back as the reply, and the
  //    caller's promise resolves with it.
  const total = await client.total('acme-industries');
  assert.deepEqual(total, { customerId: 'acme-industries', total: 150 });
  assert.deepEqual(inbox.handled, [
    { topic: TotalsConsumer.requestTopic, customerId: 'acme-industries' },
  ]);

  // 2. The reply really went through the shared producer onto the reply topic,
  //    keyed by the correlation id and marked complete — which is the same
  //    marker that makes an un-migrated ClientKafka's observable finish.
  const replies = broker
    .getSentTo(REPLY_TOPIC)
    .filter(message => message.value !== null);
  assert.equal(replies.length, 1);
  const reply = replies[0];
  assert.equal(
    reply.value,
    JSON.stringify({ customerId: 'acme-industries', total: 150 }),
  );
  assert.equal(reply.key, reply.headers?.kafka_correlationId);
  assert.notEqual(reply.headers?.['kafka_nest-is-disposed'], undefined);

  // 3. A non-retryable handler failure is a final answer, so the caller learns
  //    now instead of waiting out the timeout.
  await assert.rejects(
    client.totalExpectingRejection('nobody'),
    (error: unknown) => {
      assert.equal(error instanceof KafkaReplyRemoteError, true);
      assert.deepEqual((error as KafkaReplyRemoteError).remote, {
        name: 'BadRequestException',
        message: 'unknown customer nobody',
      });
      return true;
    },
  );

  // 4. Nobody consumes that topic, so the only signal Kafka can give is a
  //    timeout — and it means the outcome is UNKNOWN, not "it did not happen".
  await assert.rejects(
    client.totalFromNobody('acme-industries', 50),
    (error: unknown) => {
      assert.equal(error instanceof KafkaReplyTimeoutError, true);
      assert.match(String((error as Error).message), /outcome is\s+unknown/);
      return true;
    },
  );

  // 5. Fire-and-forget is untouched: an event on the same consumer class runs
  //    its handler and produces no reply at all.
  const repliesBefore = broker.getSentTo(REPLY_TOPIC).length;
  await broker.emit(TotalsConsumer.eventTopic, {
    value: JSON.stringify({ id: 'order-1' }),
  });
  await broker.idle();
  assert.equal(broker.getSentTo(REPLY_TOPIC).length, repliesBefore);

  await app.close();

  // 6. Shutdown fails new requests fast rather than draining them: the outcome
  //    of a request whose process is going away is unknowable anyway.
  await assert.rejects(client.total('acme-industries'), /shutting down/);

  console.log('Sample 07 request-reply smoke test passed.');
}

void smoke().catch((error: unknown) => {
  console.error(error);
  process.exit(1);
});
