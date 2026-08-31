import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { setTimeout as delay } from 'node:timers/promises';
import { after, before, describe, it } from 'node:test';
import { promisify } from 'node:util';
import { Controller, INestMicroservice, Injectable, Module } from '@nestjs/common';
import {
  ClientKafka,
  Ctx as NestCtx,
  KafkaContext as NestKafkaContext,
  MessagePattern,
  MicroserviceOptions,
  Payload,
  RpcException,
  Transport,
} from '@nestjs/microservices';
import { Test, TestingModule } from '@nestjs/testing';
import { lastValueFrom } from 'rxjs';
import { KafkaMessageHeaders, createConfluentDriver } from '../driver';
import { KafkaConsumer } from '../kafka-consumer.decorator';
import { KafkaHandler } from '../kafka-handler.decorator';
import { KafkaCtx, KafkaMessage } from '../kafka-params.decorators';
import { KafkaContext } from '../kafka-context';
import { KafkaModule } from '../kafka.module';
import { KafkaProducerService } from '../kafka-producer.service';
import {
  KafkaReplyRemoteError,
  KafkaReplyTimeoutError,
} from '../kafka-request-reply.errors';
import { KafkaRequestReplyService } from '../kafka-request-reply.service';

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
 * The file proves the genuinely broker-dependent behaviour the in-memory broker
 * cannot, in two suites. The first covers the transport: a real produce ->
 * consume round-trip, a real transactional commit via
 * {@link KafkaProducerService.transactional}, per-topic-concurrency plus
 * offset-commit durability (a fresh consumer in the same group does not
 * redeliver already-committed messages), and recovery across a broker restart.
 * The second covers request-reply (ADR 0001) — see its own header below. Every
 * topic and group name is unique per run, so repeated CI runs against the same
 * broker never collide.
 */

/** Brokers parsed from `KAFKA_BROKERS`; empty when unset. */
function resolveBrokers(): string[] {
  return (process.env.KAFKA_BROKERS ?? '')
    .split(',')
    .map(broker => broker.trim())
    .filter(broker => broker.length > 0);
}

const execFileAsync = promisify(execFile);

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
  listTopics(): Promise<string[]>;
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
  await warmUpUntil(producer, topic, probe => sink.seen(probe), {
    partition,
    timeoutMs,
  });
}

/**
 * The same warm-up, for a consumer this suite does not own a
 * {@link MessageSink} for — the replying handlers below, and the official
 * `ServerKafka` in the interop cases, which record probes their own way.
 *
 * `observed` is polled against the unique probe value, so it never confuses a
 * live consumer with one that merely happened to see earlier traffic.
 */
async function warmUpUntil(
  producer: KafkaProducerService,
  topic: string,
  observed: (probe: string) => boolean,
  { partition = 0, timeoutMs = 40_000 } = {},
): Promise<void> {
  const probe = `probe-${randomUUID()}`;
  const deadline = Date.now() + timeoutMs;
  while (!observed(probe)) {
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

/**
 * Name of the container running the broker, from `KAFKA_RESTART_CONTAINER`.
 *
 * The restart test has to stop and start the broker itself, which needs a
 * container it is allowed to control. That name differs between environments
 * (`kafka` in CI, `nest-kafka-broker` in `compose.yaml`), and a developer who
 * points `KAFKA_BROKERS` at a shared or managed cluster must never have it
 * restarted underneath them. The capability is therefore opt-in and named
 * explicitly rather than guessed from the broker list.
 */
const restartContainer = (process.env.KAFKA_RESTART_CONTAINER ?? '').trim();

/** The restart test additionally needs a broker it is allowed to restart. */
const skipRestart = skip || restartContainer.length === 0;

/** Reject if `work` has not settled within `timeoutMs`. */
async function withTimeout<T>(
  work: () => Promise<T>,
  timeoutMs: number,
  what: string,
): Promise<T> {
  const abort = new AbortController();
  try {
    return await Promise.race([
      work(),
      delay(timeoutMs, undefined, { signal: abort.signal }).then(() => {
        throw new Error(`Timed out after ${timeoutMs}ms: ${what}`);
      }),
    ]);
  } finally {
    abort.abort();
  }
}

/**
 * Restart the broker container out from under the running application.
 *
 * `docker restart` stops the container and starts it again, so every TCP
 * connection the client holds is severed — the closest cheap analogue of a
 * broker rolling restart or a crash.
 */
async function restartBroker(): Promise<void> {
  await withTimeout(
    () => execFileAsync('docker', ['restart', restartContainer]),
    120_000,
    `restarting container ${restartContainer}`,
  );
}

/**
 * Poll until the broker serves metadata again.
 *
 * This deliberately issues a real `listTopics()` round-trip rather than relying
 * on `admin.connect()`. librdkafka connects lazily: against a stopped broker
 * `connect()` resolves in a few milliseconds and reports success, so a
 * connect-only probe would return immediately and make the caller's wait — and
 * every assertion after it — vacuous. `listTopics()` fails with
 * `Local: Broker transport failure` while the broker is down, which is the
 * signal this loop needs.
 *
 * A throwaway admin client per attempt is intentional: one that failed while
 * the broker was down keeps reporting the stale failure rather than noticing
 * the recovered broker.
 */
async function waitForBrokerReady(timeoutMs = 120_000): Promise<void> {
  const { KafkaJS } =
    require('@confluentinc/kafka-javascript') as ConfluentModule;
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const admin = new KafkaJS.Kafka({ kafkaJS: { brokers } }).admin();
    try {
      await withTimeout(() => admin.connect(), 15_000, 'admin connect');
      await withTimeout(() => admin.listTopics(), 15_000, 'admin listTopics');
      await admin.disconnect();
      return;
    } catch (error) {
      try {
        await admin.disconnect();
      } catch {
        // The admin never reached the broker; nothing to release.
      }
      if (Date.now() > deadline) {
        throw new Error(
          `Broker did not serve metadata again within ${timeoutMs}ms: ${String(error)}`,
        );
      }
      await delay(1_000);
    }
  }
}

/**
 * Produce `value` to `topic`, retrying only while the *send itself* rejects.
 *
 * Retrying on a rejected send rather than on non-delivery keeps the delivered
 * count meaningful: once a send resolves the broker has accepted the record, so
 * the test waits for delivery instead of producing the same value again.
 */
async function sendUntilAccepted(
  producer: KafkaProducerService,
  topic: string,
  value: string,
  timeoutMs = 120_000,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    try {
      await producer.send({ topic, messages: [{ value }] });
      return;
    } catch (error) {
      if (Date.now() > deadline) {
        throw new Error(
          `Producer never recovered after the restart: ${String(error)}`,
        );
      }
      await delay(1_000);
    }
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

  it(
    'recovers the consumer and producer after the broker restarts',
    { skip: skipRestart },
    async () => {
      const topic = unique('it.restart');
      const groupId = unique('it-restart-group');
      await createTopic(topic, 1);

      @Injectable()
      @KafkaConsumer(topic, { groupId })
      class RestartConsumer {
        constructor(private readonly sink: MessageSink) {}

        @KafkaHandler()
        handle(
          @KafkaMessage() value: string,
          @KafkaCtx() context: KafkaContext,
        ) {
          this.sink.record(value, context);
        }
      }

      @Module({
        imports: [
          KafkaModule.forRoot({ clientId, client: { brokers }, driverFactory }),
        ],
        providers: [MessageSink, RestartConsumer],
      })
      class RestartModule {}

      const app: TestingModule = await Test.createTestingModule({
        imports: [RestartModule],
      }).compile();
      await app.init();

      try {
        const sink = app.get(MessageSink);
        const producer = app.get(KafkaProducerService);

        await warmUp(producer, sink, topic);

        const beforeRestart = `before-restart-${randomUUID()}`;
        await producer.send({ topic, messages: [{ value: beforeRestart }] });
        await waitFor(() => sink.seen(beforeRestart));

        // Take the broker down and bring it back underneath the running
        // application. Nothing below restarts the Nest app, re-creates the
        // producer, or re-subscribes the consumer: recovery has to come from
        // the client itself.
        await restartBroker();
        await waitForBrokerReady();

        const afterRestart = `after-restart-${randomUUID()}`;
        await sendUntilAccepted(producer, topic, afterRestart);
        await waitFor(() => sink.seen(afterRestart), { timeoutMs: 120_000 });

        assert.equal(
          sink.seen(beforeRestart),
          true,
          'the message delivered before the restart must not be lost',
        );
      } finally {
        await app.close();
      }
    },
  );

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

/**
 * What one replying handler answered, plus the raw headers of the request it
 * answered. The interop cases assert on those headers directly: they are the
 * bytes the other side actually put on the wire, not this suite's idea of them.
 */
interface HandledRequest {
  request: EchoRequest;
  headers: KafkaMessageHeaders;
}

/** The payload the replying handlers below echo back. */
interface EchoRequest {
  /** Which requester instance issued this — the affinity assertion's anchor. */
  instance?: string;
  /** Unique per request, so a reply can be traced to exactly one call. */
  nonce?: string;
  amount: number;
}

/** The reply those handlers produce. */
interface EchoReply {
  instance?: string;
  nonce?: string;
  total: number;
  via: string;
}

/**
 * The replying half the request-reply cases share: it records what it was asked
 * and answers with the doubled amount, tagged with the instance and nonce it was
 * given.
 *
 * Warm-up probes are plain text (`probe-<uuid>`) rather than JSON, and carry no
 * reply address, so they are recorded separately and answer nothing — the
 * handler runs and the reply step is skipped, which is exactly the documented
 * behaviour for mixed traffic on a request topic.
 */
@Injectable()
class ReplySink {
  readonly handled: HandledRequest[] = [];
  private readonly probes = new Set<string>();

  answer(payload: unknown, context: KafkaContext): EchoReply | null {
    if (typeof payload === 'string') {
      this.probes.add(payload);
      return null;
    }
    const request = payload as EchoRequest;
    this.handled.push({ request, headers: context.getHeaders() });
    return {
      instance: request.instance,
      nonce: request.nonce,
      total: request.amount * 2,
      via: '@nest-native/kafka',
    };
  }

  seen(probe: string): boolean {
    return this.probes.has(probe);
  }

  /** The headers of the last real (non-probe) request this handler answered. */
  lastHeaders(): KafkaMessageHeaders {
    const last = this.handled[this.handled.length - 1];
    assert.notEqual(last, undefined, 'the replier handled a real request');
    return last.headers;
  }
}

/** Read one header as text, whatever byte shape the producing side used. */
function headerText(
  headers: KafkaMessageHeaders,
  key: string,
): string | undefined {
  const raw = headers[key];
  if (raw === undefined) {
    return undefined;
  }
  const single = Array.isArray(raw) ? raw[0] : raw;
  return Buffer.isBuffer(single) ? single.toString('utf8') : single;
}

/**
 * A latch the restart case uses to hold a handler open across the outage.
 *
 * The promise is created in the constructor rather than from a field
 * initializer so `release` is captured before anything can await it.
 */
@Injectable()
class ReplyGate {
  entered = false;
  readonly opened: Promise<void>;
  private release: () => void = () => {};

  constructor() {
    this.opened = new Promise<void>(resolve => {
      this.release = resolve;
    });
  }

  open(): void {
    this.release();
  }
}

/**
 * Real-broker suite for request-reply (ADR 0001).
 *
 * These are the four cases the ADR names as genuinely broker-dependent — the
 * ones the in-memory broker cannot argue: real consumer-group coordination for
 * the ephemeral per-instance reply groups, a real outage mid-request, real
 * concurrent instances competing for one shared reply topic, and real
 * `@nestjs/microservices` on the other end of the wire in both directions.
 *
 * Gating, uniqueness, warm-up, and polling follow the same rules as the suite
 * above: `KAFKA_BROKERS` gates the file, every topic and group name is unique
 * per run, consumers are warmed up on observable delivery rather than slept on,
 * and nothing asserts on wall-clock timing beyond the deadlines the API itself
 * promises.
 */
describe('Kafka request-reply real-broker integration', { skip }, () => {
  const driverFactory = createConfluentDriver;
  const clientId = unique('nest-native-kafka-rr');

  /**
   * Stand up an application context that answers requests on `requestTopic`.
   * Deliberately configured **without** a `requestReply` block: a replier learns
   * where to answer from the request's own headers, so needing no client-side
   * configuration is part of the contract.
   */
  async function startReplier(requestTopic: string): Promise<TestingModule> {
    const groupId = unique('it-rr-replier-group');

    @Injectable()
    @KafkaConsumer(requestTopic, { groupId })
    class EchoReplier {
      constructor(private readonly sink: ReplySink) {}

      @KafkaHandler(requestTopic, { reply: true })
      echo(
        @KafkaMessage() payload: unknown,
        @KafkaCtx() context: KafkaContext,
      ): EchoReply | null {
        return this.sink.answer(payload, context);
      }
    }

    @Module({
      imports: [
        KafkaModule.forRoot({ clientId, client: { brokers }, driverFactory }),
      ],
      providers: [ReplySink, EchoReplier],
    })
    class ReplierModule {}

    const app = await Test.createTestingModule({
      imports: [ReplierModule],
    }).compile();
    await app.init();
    return app;
  }

  /** Stand up an application context that issues requests over `replyTopic`. */
  async function startRequester(replyTopic: string): Promise<TestingModule> {
    @Module({
      imports: [
        KafkaModule.forRoot({
          clientId,
          client: { brokers },
          driverFactory,
          requestReply: { replyTopic },
        }),
      ],
    })
    class RequesterModule {}

    const app = await Test.createTestingModule({
      imports: [RequesterModule],
    }).compile();
    await app.init();
    return app;
  }

  it('round-trips a request to a replying handler in another application context', async () => {
    const requestTopic = unique('it.rr.request');
    const replyTopic = unique('it.rr.reply');
    await createTopic(requestTopic, 1);
    await createTopic(replyTopic, 1);

    // The replier comes up and is proven live *first*, so that when the
    // requester starts, its very first `request()` is genuinely the first thing
    // it does after boot. That is the readiness guarantee under test: a reply
    // can come back within milliseconds, and it must not be lost to the reply
    // consumer's "latest" position still being established.
    const replier = await startReplier(requestTopic);
    const sink = replier.get(ReplySink);
    await warmUpUntil(
      replier.get(KafkaProducerService),
      requestTopic,
      probe => sink.seen(probe),
    );

    const requester = await startRequester(replyTopic);
    try {
      const nonce = randomUUID();
      const reply = await requester
        .get(KafkaRequestReplyService)
        .request<EchoReply>({
          topic: requestTopic,
          message: {
            value: JSON.stringify({ instance: 'solo', nonce, amount: 21 }),
          },
        });

      assert.deepEqual(reply.value, {
        instance: 'solo',
        nonce,
        total: 42,
        via: '@nest-native/kafka',
      });
      assert.equal(reply.topic, replyTopic);
      assert.equal(
        headerText(reply.headers, 'kafka_correlationId'),
        reply.correlationId,
        'the reply echoes the correlation id it was addressed with',
      );
      assert.equal(
        typeof reply.offset,
        'string',
        'the reply reports where it was consumed from',
      );

      // The replier saw our address, and only our address: no reply partition,
      // because this package consumes every partition of its reply topic.
      const headers = sink.lastHeaders();
      assert.equal(headerText(headers, 'kafka_replyTopic'), replyTopic);
      assert.equal(headerText(headers, 'kafka_replyPartition'), undefined);
    } finally {
      await requester.close();
      await replier.close();
    }
  });

  it(
    'settles a request that spans a broker restart, inside its own deadline',
    { skip: skipRestart },
    async () => {
      const requestTopic = unique('it.rr.restart');
      const replyTopic = unique('it.rr.restart.reply');
      const groupId = unique('it-rr-restart-group');
      await createTopic(requestTopic, 1);
      await createTopic(replyTopic, 1);

      @Injectable()
      @KafkaConsumer(requestTopic, { groupId })
      class GatedReplier {
        constructor(
          private readonly sink: ReplySink,
          private readonly gate: ReplyGate,
        ) {}

        @KafkaHandler(requestTopic, { reply: true })
        async echo(
          @KafkaMessage() payload: unknown,
          @KafkaCtx() context: KafkaContext,
        ): Promise<EchoReply | null> {
          if (typeof payload === 'string') {
            return this.sink.answer(payload, context);
          }
          // Hold the request open so the outage lands squarely between "the
          // broker accepted the request" and "the reply exists".
          this.gate.entered = true;
          await this.gate.opened;
          return this.sink.answer(payload, context);
        }
      }

      @Module({
        imports: [
          KafkaModule.forRoot({ clientId, client: { brokers }, driverFactory }),
        ],
        providers: [ReplySink, ReplyGate, GatedReplier],
      })
      class GatedReplierModule {}

      const replier = await Test.createTestingModule({
        imports: [GatedReplierModule],
      }).compile();
      await replier.init();
      const sink = replier.get(ReplySink);
      const gate = replier.get(ReplyGate);
      await warmUpUntil(
        replier.get(KafkaProducerService),
        requestTopic,
        probe => sink.seen(probe),
      );

      const requester = await startRequester(replyTopic);
      const timeoutMs = 60_000;
      try {
        const nonce = randomUUID();
        const startedAt = Date.now();
        // Settle the promise into a value either way: an in-flight rejection
        // must not become an unhandled rejection while the broker is down.
        const settled = requester
          .get(KafkaRequestReplyService)
          .request<EchoReply>(
            {
              topic: requestTopic,
              message: {
                value: JSON.stringify({ instance: 'restart', nonce, amount: 21 }),
              },
            },
            { timeoutMs },
          )
          .then(
            value => ({ status: 'resolved' as const, value }),
            (error: unknown) => ({ status: 'rejected' as const, error }),
          );

        await waitFor(() => gate.entered, { timeoutMs: 60_000 });

        // Nothing below restarts the applications, re-creates producers, or
        // re-subscribes consumers: recovery has to come from the clients.
        await restartBroker();
        await waitForBrokerReady();
        gate.open();

        const outcome = await withTimeout(
          () => settled,
          timeoutMs + 60_000,
          'the request spanning the restart to settle',
        );
        const elapsedMs = Date.now() - startedAt;

        if (outcome.status === 'resolved') {
          // Completing is the good outcome — and it must be *our* answer.
          assert.equal(outcome.value.value.nonce, nonce);
          assert.equal(outcome.value.value.total, 42);
          assert.equal(outcome.value.topic, replyTopic);
        } else {
          // Timing out is the only other honest outcome, and it must arrive on
          // time rather than hanging: a timeout means the result is unknown.
          assert.equal(
            outcome.error instanceof KafkaReplyTimeoutError,
            true,
            `expected a timeout, got ${String(outcome.error)}`,
          );
          assert.equal((outcome.error as KafkaReplyTimeoutError).timeoutMs, timeoutMs);
          assert.equal(
            elapsedMs < timeoutMs + 30_000,
            true,
            `the request settled ${elapsedMs}ms after it started, past its ${timeoutMs}ms deadline`,
          );
        }
      } finally {
        gate.open();
        await requester.close();
        await replier.close();
      }
    },
  );

  it('lands every reply at the instance that asked, never at a sibling', async () => {
    const requestTopic = unique('it.rr.affinity');
    const replyTopic = unique('it.rr.affinity.reply');
    await createTopic(requestTopic, 1);
    // Three partitions on purpose. Under the chosen routing the partition count
    // is irrelevant — every instance reads all of them — but it is exactly what
    // a shared consumer group would split across the three instances, so a
    // regression that stopped giving each instance its own ephemeral group
    // cannot pass this test by accident.
    await createTopic(replyTopic, 3);

    const replier = await startReplier(requestTopic);
    const sink = replier.get(ReplySink);
    await warmUpUntil(
      replier.get(KafkaProducerService),
      requestTopic,
      probe => sink.seen(probe),
    );

    const instances = ['alpha', 'beta', 'gamma'];
    const requestsPerInstance = 4;
    const requesters = await Promise.all(
      instances.map(() => startRequester(replyTopic)),
    );

    try {
      // All twelve requests are in flight together over one shared reply topic,
      // so every instance's reply consumer sees every other instance's replies
      // and has to drop them.
      const answered = await Promise.all(
        requesters.map((app, index) => {
          const instance = instances[index];
          const requests = app.get(KafkaRequestReplyService);
          return Promise.all(
            Array.from({ length: requestsPerInstance }, async (_, n) => {
              const nonce = `${instance}-${n}-${randomUUID()}`;
              const reply = await requests.request<EchoReply>(
                {
                  topic: requestTopic,
                  message: {
                    key: nonce,
                    value: JSON.stringify({ instance, nonce, amount: n }),
                  },
                },
                { timeoutMs: 60_000 },
              );
              return {
                nonce,
                reply,
                expected: {
                  instance,
                  nonce,
                  total: n * 2,
                  via: '@nest-native/kafka',
                },
              };
            }),
          );
        }),
      );

      const correlationIds = new Set<string>();
      for (const [index, perInstance] of answered.entries()) {
        const instance = instances[index];
        assert.equal(perInstance.length, requestsPerInstance);

        for (const { nonce, reply, expected } of perInstance) {
          // The claim the whole routing decision rests on: what came back here
          // is this instance's answer to this call — not a sibling's reply, not
          // another call's, and not this instance's own readiness sentinel.
          assert.deepEqual(
            reply.value,
            expected,
            `instance "${instance}" resolved ${JSON.stringify(reply.value)} ` +
              `as the answer to its request "${nonce}"`,
          );
          assert.equal(reply.topic, replyTopic);
          correlationIds.add(reply.correlationId);
        }
      }

      assert.equal(
        correlationIds.size,
        instances.length * requestsPerInstance,
        'every request was settled by a distinct correlation id',
      );
      assert.equal(
        sink.handled.length,
        instances.length * requestsPerInstance,
        'the replier answered exactly the requests that were issued',
      );
    } finally {
      await Promise.all(requesters.map(app => app.close()));
      await replier.close();
    }
  });

  it('resolves a reply produced by a real @nestjs/microservices ServerKafka', async () => {
    const requestTopic = unique('it.rr.interop.server');
    const failingTopic = unique('it.rr.interop.server-error');
    const replyTopic = unique('it.rr.interop.server.reply');
    await createTopic(requestTopic, 1);
    await createTopic(failingTopic, 1);
    await createTopic(replyTopic, 1);

    /**
     * An un-migrated service: the official transport, the official decorators,
     * `kafkajs` underneath. Nothing here knows this package exists.
     */
    @Controller()
    class OfficialController {
      readonly seen: unknown[] = [];
      readonly headers: Record<string, unknown>[] = [];

      @MessagePattern(requestTopic)
      total(
        @Payload() query: EchoRequest | string,
        @NestCtx() context: NestKafkaContext,
      ): unknown {
        this.seen.push(query);
        this.headers.push(context.getMessage().headers ?? {});
        if (typeof query === 'string') {
          return null;
        }
        return { total: query.amount * 2, via: 'ServerKafka' };
      }

      @MessagePattern(failingTopic)
      explode(): never {
        throw new RpcException('official handler exploded');
      }
    }

    const official = await Test.createTestingModule({
      controllers: [OfficialController],
    }).compile();
    const microservice: INestMicroservice =
      official.createNestMicroservice<MicroserviceOptions>({
        transport: Transport.KAFKA,
        options: {
          client: { clientId: unique('official'), brokers },
          consumer: { groupId: unique('official-server-group') },
        },
        logger: false,
      });
    await microservice.listen();

    const requester = await startRequester(replyTopic);
    try {
      const controller = official.get(OfficialController);
      const producer = requester.get(KafkaProducerService);
      // A message with no correlation id is an event to `ServerKafka`, so the
      // warm-up probe reaches the same handler and proves the same assignment.
      await warmUpUntil(producer, requestTopic, probe =>
        controller.seen.includes(probe),
      );
      await warmUpUntil(producer, failingTopic, () =>
        controller.seen.some(seen => typeof seen === 'string'),
      );

      const requests = requester.get(KafkaRequestReplyService);
      const reply = await requests.request<{ total: number; via: string }>({
        topic: requestTopic,
        message: { value: JSON.stringify({ amount: 21 }) },
      });

      // The official transport answered us, byte for byte.
      assert.deepEqual(reply.value, { total: 42, via: 'ServerKafka' });
      assert.equal(reply.topic, replyTopic);
      assert.equal(
        headerText(reply.headers, 'kafka_correlationId'),
        reply.correlationId,
        'ServerKafka echoes the correlation id we stamped',
      );
      assert.notEqual(
        reply.headers['kafka_nest-is-disposed'],
        undefined,
        'ServerKafka marks a settled reply with its completion header',
      );

      // What `ServerKafka` actually received: a JSON body, our reply topic, and
      // no reply partition. The last one is the load-bearing interop claim —
      // `assignReplyPartition` early-returns on a nil header, so omitting it
      // leaves the reply to the default partitioner, which is what we want.
      assert.deepEqual(controller.seen[controller.seen.length - 1], {
        amount: 21,
      });
      const received =
        controller.headers[controller.headers.length - 1] ?? {};
      assert.equal(String(received.kafka_replyTopic), replyTopic);
      assert.equal(received.kafka_replyPartition, undefined);

      // The error direction of the same contract: `kafka_nest-err` carries the
      // official transport's serialized RPC error, and we surface it as a
      // remote error rather than as a timeout.
      await assert.rejects(
        requests.request({
          topic: failingTopic,
          message: { value: JSON.stringify({ amount: 1 }) },
        }),
        (error: unknown) => {
          assert.equal(error instanceof KafkaReplyRemoteError, true);
          assert.deepEqual((error as KafkaReplyRemoteError).remote, {
            status: 'error',
            message: 'official handler exploded',
          });
          return true;
        },
      );
    } finally {
      await requester.close();
      await microservice.close();
      await official.close();
    }
  });

  it('answers a real @nestjs/microservices ClientKafka.send() so its observable completes', async () => {
    const requestTopic = unique('it.rr.interop.client');
    // The old client derives this name itself and cannot be told otherwise.
    const nestReplyTopic = `${requestTopic}.reply`;
    await createTopic(requestTopic, 1);
    await createTopic(nestReplyTopic, 1);

    const replier = await startReplier(requestTopic);
    const client = new ClientKafka({
      client: { clientId: unique('official'), brokers },
      consumer: { groupId: unique('official-client-group') },
    });

    try {
      const sink = replier.get(ReplySink);
      await warmUpUntil(
        replier.get(KafkaProducerService),
        requestTopic,
        probe => sink.seen(probe),
      );

      client.subscribeToResponseOf(requestTopic);
      await client.connect();
      // `ClientKafka.send()` throws client-side until its reply-partition
      // assignment exists, so wait for the group join rather than racing it.
      await waitFor(
        () => client.getConsumerAssignments()[nestReplyTopic] !== undefined,
        { timeoutMs: 60_000 },
      );

      const answer = await withTimeout(
        () =>
          lastValueFrom(
            client.send<EchoReply, EchoRequest>(requestTopic, { amount: 21 }),
          ),
        60_000,
        "the ClientKafka observable to emit our reply and complete",
      );

      assert.deepEqual(answer, { total: 42, via: '@nest-native/kafka' });

      // What the old client put on the wire, and what our replier answered
      // without a single line of request-reply configuration: its own derived
      // reply topic and an explicit reply partition it owns.
      const headers = sink.lastHeaders();
      assert.equal(headerText(headers, 'kafka_replyTopic'), nestReplyTopic);
      const advertised = headerText(headers, 'kafka_replyPartition');
      assert.equal(
        Number.isInteger(Number(advertised)) && Number(advertised) >= 0,
        true,
        `expected an explicit reply partition, got ${String(advertised)}`,
      );
      assert.notEqual(
        headerText(headers, 'kafka_correlationId'),
        undefined,
        'the old client correlates every request it sends',
      );
    } finally {
      await client.close();
      await replier.close();
    }
  });

  before(() => {
    assert.equal(brokers.length > 0, true);
  });
});
