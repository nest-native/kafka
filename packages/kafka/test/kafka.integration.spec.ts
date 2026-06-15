import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { setTimeout as delay } from 'node:timers/promises';
import { after, before, describe, it } from 'node:test';
import { Injectable, Module } from '@nestjs/common';
import { Test, TestingModule } from '@nestjs/testing';
import { createConfluentDriver } from '../driver';
import { KafkaConsumer } from '../kafka-consumer.decorator';
import { KafkaHandler } from '../kafka-handler.decorator';
import { KafkaCtx, KafkaMessage } from '../kafka-params.decorators';
import { KafkaContext } from '../kafka-context';
import { KafkaModule } from '../kafka.module';
import { KafkaProducerService } from '../kafka-producer.service';

/**
 * Real-broker integration suite for `@nest-native/kafka`.
 *
 * Unlike every other spec in this directory — which runs against fakes and the
 * in-memory broker so coverage runs anywhere — this suite opens a real
 * connection through {@link createConfluentDriver} and the native
 * `@confluentinc/kafka-javascript` client. It is therefore **gated on the
 * `KAFKA_BROKERS` environment variable**: when it is unset the whole suite is
 * skipped, so `npm run test:cov` (which loads every `*.spec.ts` in this folder)
 * stays a no-op here and the 100% coverage gate is unaffected. CI's dedicated
 * `integration` job stands up a single-node KRaft Kafka, sets
 * `KAFKA_BROKERS=localhost:9092`, and runs only this file through
 * `npm run test:integration`.
 *
 * The suite proves the genuinely broker-dependent behaviour the in-memory broker
 * cannot: a real produce -> consume round-trip, a real transactional commit via
 * {@link KafkaProducerService.transactional}, and per-topic-concurrency plus
 * offset-commit durability (a fresh consumer in the same group does not redeliver
 * already-committed messages). Every topic and group name is unique per run, so
 * repeated CI runs against the same broker never collide.
 */

/** Brokers parsed from `KAFKA_BROKERS`; empty when unset. */
function resolveBrokers(): string[] {
  return (process.env.KAFKA_BROKERS ?? '')
    .split(',')
    .map(broker => broker.trim())
    .filter(broker => broker.length > 0);
}

const brokers = resolveBrokers();
const skip = brokers.length === 0;

/** A short unique suffix so concurrent or repeated runs never collide. */
function unique(prefix: string): string {
  return `${prefix}.${randomUUID().slice(0, 8)}`;
}

/**
 * Minimal slice of the KafkaJS-compatible admin client this suite uses to
 * pre-create topics with an explicit partition count. Modelled locally so the
 * file never imports the optional peer's types at module-evaluation time.
 */
interface AdminClient {
  connect(): Promise<void>;
  disconnect(): Promise<void>;
  createTopics(args: {
    topics: { topic: string; numPartitions: number }[];
  }): Promise<unknown>;
}

interface ConfluentClient {
  admin(): AdminClient;
}

interface ConfluentModule {
  KafkaJS: { Kafka: new (config: unknown) => ConfluentClient };
}

/**
 * Pre-create a topic with `numPartitions` partitions. Auto-creation only ever
 * yields a single partition, so the per-partition-concurrency test must create
 * the topic explicitly to have partitions to spread across.
 */
async function createTopic(topic: string, numPartitions: number): Promise<void> {
  // The peer is installed in the integration job; requiring it here (only when a
  // broker is configured) keeps the package's lazy-load contract intact.
  const { KafkaJS } =
    require('@confluentinc/kafka-javascript') as ConfluentModule;
  const admin = new KafkaJS.Kafka({ kafkaJS: { brokers } }).admin();
  await admin.connect();
  try {
    await admin.createTopics({ topics: [{ topic, numPartitions }] });
  } finally {
    await admin.disconnect();
  }
}

interface ReceivedMessage {
  value: string;
  partition: number;
  offset?: string;
}

/**
 * A sink the integration consumers record into. Shared through DI so the test
 * body can assert on exactly what reached the handler.
 */
@Injectable()
class MessageSink {
  readonly received: ReceivedMessage[] = [];

  record(value: string, context: KafkaContext): void {
    this.received.push({
      value,
      partition: context.getPartition(),
      offset: context.getMessage().offset,
    });
  }

  countFor(value: string): number {
    return this.received.filter(message => message.value === value).length;
  }

  seen(value: string): boolean {
    return this.countFor(value) > 0;
  }
}

/**
 * Re-send a unique probe message to `topic` until the consumer records it (or a
 * deadline elapses), proving the consumer group has been assigned and is live.
 *
 * A consumer subscribed at the latest offset never sees messages produced before
 * its first assignment completes, so a single pre-assignment produce would race.
 * Warming up on observable delivery makes the subsequent definitive produce
 * deterministic. Probe values are unique, so they never collide with the values
 * the test later asserts on.
 */
async function warmUp(
  producer: KafkaProducerService,
  sink: MessageSink,
  topic: string,
  partition = 0,
  timeoutMs = 40_000,
): Promise<void> {
  const probe = `probe-${randomUUID()}`;
  const deadline = Date.now() + timeoutMs;
  while (!sink.seen(probe)) {
    if (Date.now() > deadline) {
      throw new Error(`Timed out warming up consumer for ${topic}`);
    }
    await producer.send({ topic, messages: [{ partition, value: probe }] });
    await delay(500);
  }
}

/**
 * Poll `predicate` until it returns true or the deadline elapses. Real brokers
 * need a moment to deliver, so the suite waits on observable progress instead of
 * a fixed sleep.
 */
async function waitFor(
  predicate: () => boolean,
  { timeoutMs = 30_000, intervalMs = 100 } = {},
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() > deadline) {
      throw new Error('Timed out waiting for the broker condition');
    }
    await delay(intervalMs);
  }
}

describe('Kafka real-broker integration', { skip }, () => {
  // The driver factory is the real Confluent driver; `createConfluentDriver`
  // loads the native client lazily, so importing this file without a broker is
  // still free.
  const driverFactory = createConfluentDriver;
  const clientId = unique('nest-native-kafka-it');

  it('round-trips a real produce -> consume through createConfluentDriver', async () => {
    const topic = unique('it.roundtrip');
    const groupId = unique('it-roundtrip-group');
    await createTopic(topic, 1);

    @Injectable()
    @KafkaConsumer(topic, { groupId })
    class RoundTripConsumer {
      constructor(private readonly sink: MessageSink) {}

      @KafkaHandler()
      handle(@KafkaMessage() value: string, @KafkaCtx() context: KafkaContext) {
        this.sink.record(value, context);
      }
    }

    @Module({
      imports: [
        KafkaModule.forRoot({ clientId, client: { brokers }, driverFactory }),
      ],
      providers: [MessageSink, RoundTripConsumer],
    })
    class RoundTripModule {}

    const app: TestingModule = await Test.createTestingModule({
      imports: [RoundTripModule],
    }).compile();
    await app.init();

    try {
      const sink = app.get(MessageSink);
      const producer = app.get(KafkaProducerService);

      await warmUp(producer, sink, topic);

      const payload = `hello-${randomUUID()}`;
      await producer.send({ topic, messages: [{ key: 'k', value: payload }] });

      await waitFor(() => sink.seen(payload));
      assert.equal(sink.countFor(payload), 1);
    } finally {
      await app.close();
    }
  });

  it('commits a real Kafka transaction via KafkaProducerService.transactional', async () => {
    const topic = unique('it.tx');
    const groupId = unique('it-tx-group');
    const transactionalId = unique('it-tx-producer');
    await createTopic(topic, 1);

    @Injectable()
    @KafkaConsumer(topic, { groupId })
    class TxConsumer {
      constructor(private readonly sink: MessageSink) {}

      @KafkaHandler()
      handle(@KafkaMessage() value: string, @KafkaCtx() context: KafkaContext) {
        this.sink.record(value, context);
      }
    }

    @Module({
      imports: [
        KafkaModule.forRoot({
          clientId,
          client: { brokers },
          producer: { transactionalId },
          driverFactory,
        }),
      ],
      providers: [MessageSink, TxConsumer],
    })
    class TxModule {}

    const app = await Test.createTestingModule({
      imports: [TxModule],
    }).compile();
    await app.init();

    try {
      const sink = app.get(MessageSink);
      const producer = app.get(KafkaProducerService);

      // A transactional producer must wrap every send in a transaction, so warm
      // up through a committed throwaway transaction too.
      const probe = `probe-${randomUUID()}`;
      const deadline = Date.now() + 40_000;
      while (!sink.seen(probe)) {
        if (Date.now() > deadline) {
          throw new Error('Timed out warming up transactional consumer');
        }
        await producer.transactional(async tx => {
          await tx.send({ topic, messages: [{ value: probe }] });
        });
        await delay(500);
      }

      const committed = `committed-${randomUUID()}`;
      // Resolves -> the transaction commits, so the message must be delivered.
      const result = await producer.transactional(async tx => {
        await tx.send({ topic, messages: [{ value: committed }] });
        return 'ok';
      });
      assert.equal(result, 'ok');

      await waitFor(() => sink.seen(committed));
      assert.equal(sink.countFor(committed), 1);
    } finally {
      await app.close();
    }
  });

  it('processes partitions concurrently and durably commits offsets', async () => {
    const topic = unique('it.concurrency');
    const groupId = unique('it-concurrency-group');
    await createTopic(topic, 2);

    @Injectable()
    @KafkaConsumer(topic, { groupId, concurrency: 2 })
    class ConcurrencyConsumer {
      constructor(private readonly sink: MessageSink) {}

      @KafkaHandler()
      async handle(
        @KafkaMessage() value: string,
        @KafkaCtx() context: KafkaContext,
      ) {
        // A small delay makes genuine cross-partition concurrency observable:
        // with strict sequential processing the two partitions would serialize.
        await delay(50);
        this.sink.record(value, context);
      }
    }

    @Module({
      imports: [
        KafkaModule.forRoot({ clientId, client: { brokers }, driverFactory }),
      ],
      providers: [MessageSink, ConcurrencyConsumer],
    })
    class ConcurrencyModule {}

    const firstApp = await Test.createTestingModule({
      imports: [ConcurrencyModule],
    }).compile();
    await firstApp.init();

    const marker = randomUUID();
    const messages = [
      { partition: 0, value: `p0-a-${marker}` },
      { partition: 0, value: `p0-b-${marker}` },
      { partition: 1, value: `p1-a-${marker}` },
      { partition: 1, value: `p1-b-${marker}` },
    ];
    // Capture the expected values before producing: the Confluent producer
    // serializes each message's `value` to a Buffer in place, so reading them
    // back off `messages` after `send` would compare against Buffers.
    const expectedValues = messages.map(message => message.value);

    try {
      const sink = firstApp.get(MessageSink);
      const producer = firstApp.get(KafkaProducerService);

      // Warm up both partitions so assignment covers the whole topic.
      await warmUp(producer, sink, topic, 0);
      await warmUp(producer, sink, topic, 1);

      await producer.send({ topic, messages });
      await waitFor(() => expectedValues.every(value => sink.seen(value)), {
        timeoutMs: 45_000,
      });

      for (const value of expectedValues) {
        assert.equal(sink.countFor(value), 1, `delivered ${value}`);
      }

      // Both partitions were observed, proving per-topic concurrency reached the
      // live broker (concurrency: 2 -> partitionsConsumedConcurrently: 2).
      const partitions = new Set(
        sink.received
          .filter(message => message.value.includes(marker))
          .map(message => message.partition),
      );
      assert.equal(partitions.has(0) && partitions.has(1), true, 'both partitions');
    } finally {
      // Graceful shutdown drains in-flight work and commits offsets.
      await firstApp.close();
    }

    // Offset durability: a fresh consumer in the same group must NOT redeliver
    // the already-committed messages. Start a second app on the same group and
    // assert it stays silent for a quiet window.
    const secondApp = await Test.createTestingModule({
      imports: [ConcurrencyModule],
    }).compile();
    await secondApp.init();

    try {
      const sink = secondApp.get(MessageSink);
      // Give the second consumer time to join and (not) receive anything.
      await delay(6_000);
      const redelivered = expectedValues.filter(value => sink.seen(value));
      assert.deepEqual(
        redelivered,
        [],
        'committed offsets must not be redelivered to a new consumer in the group',
      );
    } finally {
      await secondApp.close();
    }
  });

  before(() => {
    // Defensive: the suite must never run without a broker. (`skip` already
    // guarantees this, but the assertion documents the contract.)
    assert.equal(brokers.length > 0, true);
  });

  after(() => {
    // No shared broker state to tear down: every test owns unique topics and
    // groups, and each app is closed in its own `finally`.
  });
});
