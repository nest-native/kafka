import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { type DynamicModule, Injectable, Module, type Type } from '@nestjs/common';
import { Test, type TestingModule } from '@nestjs/testing';
import {
  type InMemoryOutboxStore,
  NonRetryableMessageError,
  Outbox,
  OutboxDeadLetters,
  type OutboxEnvelope,
  type OutboxEvent,
  OutboxEvents,
  OutboxInbox,
  OutboxModule,
  OutboxRelay,
  OutboxStorage,
} from '@nestjs/outbox';
import type { KafkaDriverFactory, KafkaDriverProducer } from '../driver';
import { KafkaConsumer } from '../kafka-consumer.decorator';
import { KafkaContext } from '../kafka-context';
import { KafkaHandler } from '../kafka-handler.decorator';
import { KafkaCtx, KafkaMessage } from '../kafka-params.decorators';
import { KafkaProducerService } from '../kafka-producer.service';
import { KafkaModule } from '../kafka.module';
import { KafkaOutboxTransport } from '../outbox';
import { InMemoryKafkaBroker } from '../testing/in-memory-kafka-broker';
import { KafkaTestModule } from '../testing/kafka-test.module';

/**
 * The transport inside the official `OutboxModule`, in real Nest DI, end to
 * end without a broker: `Outbox.add()` in a transaction of the outbox's
 * in-memory store, the relay (flushed with `runOnce()`), `KafkaOutboxTransport`,
 * the package's `InMemoryKafkaBroker`, and a `@KafkaHandler` consumer that reads
 * the envelope and deduplicates through `OutboxInbox`.
 */

interface OrderPlaced {
  orderId: string;
  total: number;
}

const ORDERS = 'orders.placed';

/** What the billing consumer saw, and what it did. */
@Injectable()
class Billing {
  readonly deliveries: { topic: string; envelope: OutboxEnvelope<OrderPlaced> }[] = [];
  readonly charged: string[] = [];
}

/**
 * A billing consumer of `topic`: every delivery is recorded, and the charge runs
 * once per envelope id, through the outbox's inbox.
 */
function billingConsumer(topic: string): Type {
  @Injectable()
  @KafkaConsumer(topic, { groupId: 'billing' })
  class BillingConsumer {
    constructor(
      private readonly billing: Billing,
      private readonly inbox: OutboxInbox,
    ) {}

    @KafkaHandler()
    async onOrderPlaced(
      @KafkaMessage() envelope: OutboxEnvelope<OrderPlaced>,
      @KafkaCtx() context: KafkaContext,
    ): Promise<void> {
      this.billing.deliveries.push({ topic: context.getTopic(), envelope });
      await this.inbox.process('billing', envelope.id, () => {
        this.billing.charged.push(envelope.payload.orderId);
      });
    }
  }
  return BillingConsumer;
}

interface Harness {
  app: TestingModule;
  billing: Billing;
  relay: OutboxRelay;
  events: OutboxEvent[];
  /** `Outbox.add()` in a transaction of the in-memory store, as an app would in its own. */
  add(order: OrderPlaced, key?: string): Promise<string>;
}

async function start(
  kafka: DynamicModule,
  outbox: DynamicModule,
  consumes = ORDERS,
): Promise<Harness> {
  @Module({
    imports: [kafka, outbox],
    providers: [Billing, billingConsumer(consumes)],
  })
  class AppModule {}

  const app = await Test.createTestingModule({ imports: [AppModule] }).compile();
  await app.init();

  const events: OutboxEvent[] = [];
  app.get(OutboxEvents).events$.subscribe(event => events.push(event));
  const store = app.get(OutboxStorage).messages as InMemoryOutboxStore;
  const writer = app.get(Outbox);

  return {
    app,
    billing: app.get(Billing),
    relay: app.get(OutboxRelay),
    events,
    add: async (order, key = order.orderId) => {
      const added = await store.transaction(tx =>
        writer.add(tx, {
          topic: ORDERS,
          key,
          payload: order,
          headers: { 'x-tenant': 'acme' },
        }),
      );
      return added.id;
    },
  };
}

/** The outbox's own options for these tests: no poll loop, `runOnce()` drives it. */
const manualRelay = { relay: { enabled: false } } as const;

/**
 * The in-memory broker's driver, with a producer that throws the next queued
 * error for a topic before delivering anything to it.
 */
function failingDriver(
  broker: InMemoryKafkaBroker,
  failures: Map<string, unknown[]>,
): KafkaDriverFactory {
  return () => {
    const driver = broker.createDriver();
    return {
      ...driver,
      createProducer: (): KafkaDriverProducer => {
        const producer = driver.createProducer();
        return {
          ...producer,
          send: async record => {
            const queued = failures.get(record.topic) ?? [];
            if (queued.length > 0) {
              throw queued.shift();
            }
            return producer.send(record);
          },
        };
      },
    };
  };
}

function kafkaCode(code: number, message: string): Error {
  return Object.assign(new Error(message), { code });
}

describe('KafkaOutboxTransport in the official OutboxModule', () => {
  it('runs as the class in forRoot: Nest builds it with the KafkaModule producer, and the relay delivers to a @KafkaHandler', async () => {
    const broker = new InMemoryKafkaBroker();
    const { app, billing, relay, add } = await start(
      KafkaTestModule.forRoot({ broker }),
      OutboxModule.forRoot({
        transports: { kafka: KafkaOutboxTransport },
        ...manualRelay,
      }),
    );
    try {
      const id = await add({ orderId: 'order-1', total: 42 });

      const run = await relay.runOnce();
      await broker.idle();

      assert.equal(run.claimed, 1);
      assert.equal(run.published, 1);
      assert.deepEqual(billing.charged, ['order-1']);
      assert.equal(billing.deliveries.length, 1);
      const { topic, envelope } = billing.deliveries[0];
      assert.equal(topic, ORDERS);
      assert.equal(envelope.id, id);
      assert.equal(envelope.topic, ORDERS);
      assert.equal(envelope.key, 'order-1');
      assert.deepEqual(envelope.headers, { 'x-tenant': 'acme' });
      assert.deepEqual(envelope.payload, { orderId: 'order-1', total: 42 });

      const [record] = broker.getSentTo(ORDERS);
      assert.equal(record.key, 'order-1');
      assert.deepEqual(record.headers, { 'x-tenant': 'acme' });
      assert.equal(record.value, JSON.stringify(envelope));
    } finally {
      await app.close();
    }
  });

  it('runs as a class configured with with() in forRoot', async () => {
    const broker = new InMemoryKafkaBroker();
    const { app, billing, relay, add } = await start(
      KafkaTestModule.forRoot({ broker }),
      OutboxModule.forRoot({
        transports: {
          kafka: KafkaOutboxTransport.with({
            toRecord: message => ({ topic: `staging.${message.topic}` }),
          }),
        },
        ...manualRelay,
      }),
      `staging.${ORDERS}`,
    );
    try {
      const id = await add({ orderId: 'order-2', total: 7 });

      assert.equal((await relay.runOnce()).published, 1);
      await broker.idle();

      assert.equal(broker.getSentTo(ORDERS).length, 0);
      assert.equal(broker.getSentTo(`staging.${ORDERS}`).length, 1);
      assert.equal(billing.deliveries[0].topic, `staging.${ORDERS}`);
      // Only the topic changed: the envelope still names the outbox topic.
      assert.equal(billing.deliveries[0].envelope.id, id);
      assert.equal(billing.deliveries[0].envelope.topic, ORDERS);
    } finally {
      await app.close();
    }
  });

  it("runs as an instance built in forRootAsync's factory", async () => {
    const broker = new InMemoryKafkaBroker();
    const { app, billing, relay, add } = await start(
      KafkaTestModule.forRoot({ broker }),
      OutboxModule.forRootAsync({
        inject: [KafkaProducerService],
        useFactory: (producer: KafkaProducerService) => ({
          transports: {
            kafka: new KafkaOutboxTransport(producer, {
              toRecord: message => ({
                topic: `dev.${message.topic}`,
                headers: { ...message.headers, 'x-source': 'outbox' },
              }),
            }),
          },
          ...manualRelay,
        }),
      }),
      `dev.${ORDERS}`,
    );
    try {
      await add({ orderId: 'order-3', total: 1 });

      assert.equal((await relay.runOnce()).published, 1);
      await broker.idle();

      const [record] = broker.getSentTo(`dev.${ORDERS}`);
      assert.equal(record.key, 'order-3');
      assert.deepEqual(record.headers, { 'x-tenant': 'acme', 'x-source': 'outbox' });
      assert.deepEqual(billing.charged, ['order-3']);
    } finally {
      await app.close();
    }
  });

  it('lets the consumer drop a redelivered record through OutboxInbox, by the envelope id', async () => {
    const broker = new InMemoryKafkaBroker();
    const { app, billing, relay, add } = await start(
      KafkaTestModule.forRoot({ broker }),
      OutboxModule.forRoot({
        transports: { kafka: KafkaOutboxTransport },
        ...manualRelay,
      }),
    );
    try {
      await add({ orderId: 'order-4', total: 3 });
      await relay.runOnce();
      await broker.idle();

      // Kafka redelivers the same record: after a rebalance, or a relay retry
      // whose first attempt landed after its timeout.
      const [record] = broker.getSentTo(ORDERS);
      await broker.emit(ORDERS, record);
      await broker.idle();

      assert.equal(billing.deliveries.length, 2);
      assert.deepEqual(billing.charged, ['order-4']);
    } finally {
      await app.close();
    }
  });

  it('dead-letters a record Kafka refuses permanently on the first attempt, with the refusal as its cause', async () => {
    const broker = new InMemoryKafkaBroker();
    const tooLarge = kafkaCode(10, 'Broker: Message size too large');
    const { app, billing, relay, add, events } = await start(
      KafkaModule.forRoot({
        driverFactory: failingDriver(broker, new Map([[ORDERS, [tooLarge]]])),
      }),
      OutboxModule.forRoot({
        transports: { kafka: KafkaOutboxTransport },
        // A budget the relay would spend on a retryable failure.
        retry: { attempts: 5, backoff: () => 0 },
        ...manualRelay,
      }),
    );
    try {
      const id = await add({ orderId: 'order-5', total: 9 });

      const run = await relay.runOnce();
      await broker.idle();

      assert.equal(run.deadLettered, 1);
      assert.equal(run.retried, 0);
      const [letter, ...others] = await app.get(OutboxDeadLetters).list();
      assert.equal(others.length, 0);
      assert.equal(letter.id, id);
      assert.equal(letter.reason, 'rejected');
      assert.equal(letter.attempts, 1);
      assert.match(
        String(letter.lastError),
        /^NonRetryableMessageError: Kafka refused the record for topic "orders\.placed" permanently \(MSG_SIZE_TOO_LARGE, code 10\): Broker: Message size too large$/,
      );
      const deadLettered = events.find(event => event.type === 'dead-lettered');
      assert.ok(deadLettered?.type === 'dead-lettered');
      assert.equal(deadLettered.error instanceof NonRetryableMessageError, true);
      assert.equal((deadLettered.error as { cause?: unknown }).cause, tooLarge);

      // Nothing was produced, and the relay has nothing left to try.
      assert.equal(broker.getSentTo(ORDERS).length, 0);
      assert.equal(billing.deliveries.length, 0);
      assert.equal((await relay.runOnce()).claimed, 0);
    } finally {
      await app.close();
    }
  });

  it('rethrows any other failure for the relay to retry, and delivers on the next attempt', async () => {
    const broker = new InMemoryKafkaBroker();
    const timedOut = kafkaCode(-192, 'Local: Message timed out');
    const { app, billing, relay, add, events } = await start(
      KafkaModule.forRoot({
        driverFactory: failingDriver(broker, new Map([[ORDERS, [timedOut]]])),
      }),
      OutboxModule.forRoot({
        transports: { kafka: KafkaOutboxTransport },
        retry: { attempts: 5, backoff: () => 0 },
        ...manualRelay,
      }),
    );
    try {
      const id = await add({ orderId: 'order-6', total: 2 });

      const first = await relay.runOnce();
      assert.equal(first.retried, 1);
      assert.equal(first.deadLettered, 0);
      const retry = events.find(event => event.type === 'retry-scheduled');
      assert.ok(retry?.type === 'retry-scheduled');
      assert.equal(retry.error, timedOut);
      assert.equal(billing.deliveries.length, 0);

      const second = await relay.runOnce();
      await broker.idle();

      assert.equal(second.published, 1);
      assert.deepEqual(await app.get(OutboxDeadLetters).list(), []);
      assert.equal(billing.deliveries[0].envelope.id, id);
      assert.deepEqual(billing.charged, ['order-6']);
    } finally {
      await app.close();
    }
  });

  it('names the missing producer when no KafkaModule is there to provide it', async () => {
    await assert.rejects(
      Test.createTestingModule({
        imports: [
          OutboxModule.forRoot({
            transports: { kafka: KafkaOutboxTransport },
            ...manualRelay,
          }),
        ],
      }).compile(),
      /KafkaOutboxTransport[\s\S]*KafkaProducerService/,
    );
  });
});
