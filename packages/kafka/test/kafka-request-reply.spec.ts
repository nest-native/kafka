import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import {
  ArgumentsHost,
  BadRequestException,
  CallHandler,
  Catch,
  ExceptionFilter,
  ExecutionContext,
  Injectable,
  Module,
  NestInterceptor,
  UseFilters,
  UseInterceptors,
} from '@nestjs/common';
import { Test } from '@nestjs/testing';
import { Observable, map } from 'rxjs';
import { KafkaMessageHeaders, KafkaProducerMessage } from '../driver';
import { KafkaConsumer } from '../kafka-consumer.decorator';
import { KafkaHandler } from '../kafka-handler.decorator';
import { KafkaCtx, KafkaMessage } from '../kafka-params.decorators';
import { KafkaContext } from '../kafka-context';
import { KafkaErrorBehavior } from '../kafka-error-mapping';
import { KafkaModule } from '../kafka.module';
import { KafkaRequestReplyService } from '../kafka-request-reply.service';
import {
  KafkaReplyAbortedError,
  KafkaReplyDeliveryError,
  KafkaReplyRemoteError,
  KafkaReplyTimeoutError,
} from '../kafka-request-reply.errors';
import {
  DEFAULT_KAFKA_REQUEST_REPLY_HEADERS as KEYS,
  KAFKA_READINESS_PROBE_HEADER,
} from '../kafka-request-reply.protocol';
import { KAFKA_TEST_BROKER } from '../tokens';
import { InMemoryKafkaBroker } from '../testing/in-memory-kafka-broker';
import { KafkaTestModule } from '../testing/kafka-test.module';

const REPLY_TOPIC = 'app.replies';

interface TotalQuery {
  customerId: string;
}

@Injectable()
@KafkaConsumer(undefined, { groupId: 'totals-service' })
class TotalsConsumer {
  readonly seen: string[] = [];

  @KafkaHandler('orders.total', { reply: true })
  total(@KafkaMessage() query: TotalQuery): { total: number } {
    this.seen.push(query.customerId);
    return { total: query.customerId.length };
  }

  /** A replying handler whose value needs no serialization. */
  @KafkaHandler('orders.name', { reply: true })
  name(): string {
    return 'plain text';
  }

  /** A replying handler that answers with nothing at all. */
  @KafkaHandler('orders.void', { reply: true })
  void(): void {}

  /** Non-retryable: the 4xx maps to 'commit', so the caller gets an answer. */
  @KafkaHandler('orders.rejected', { reply: true })
  rejected(): never {
    throw new BadRequestException('unknown customer');
  }

  /** Retryable: mapped to 'retry', so no reply exists to send. */
  @KafkaHandler('orders.flaky', { reply: true })
  flaky(@KafkaMessage() query: TotalQuery): string {
    this.seen.push(query.customerId);
    if (this.seen.filter(id => id === query.customerId).length === 1) {
      throw new Error('downstream unavailable');
    }
    return 'recovered';
  }
}

@Injectable()
class DoublingInterceptor implements NestInterceptor {
  intercept(_context: ExecutionContext, next: CallHandler): Observable<unknown> {
    return next
      .handle()
      .pipe(map(value => ({ ...(value as object), intercepted: true })));
  }
}

@Catch(BadRequestException)
class AnsweringFilter implements ExceptionFilter {
  catch(exception: BadRequestException, _host: ArgumentsHost): unknown {
    return { handled: exception.message };
  }
}

@Injectable()
@KafkaConsumer(undefined, { groupId: 'enhanced-service' })
class EnhancedConsumer {
  @KafkaHandler('orders.intercepted', { reply: true })
  @UseInterceptors(DoublingInterceptor)
  intercepted(): { total: number } {
    return { total: 1 };
  }

  @KafkaHandler('orders.filtered', { reply: true })
  @UseFilters(AnsweringFilter)
  filtered(): never {
    throw new BadRequestException('caught by the filter');
  }
}

/** A fire-and-forget handler on a topic that also carries requests. */
@Injectable()
@KafkaConsumer(undefined, { groupId: 'silent-service' })
class SilentConsumer {
  readonly seen: unknown[] = [];

  @KafkaHandler('orders.silent')
  handle(@KafkaMessage() payload: unknown): void {
    this.seen.push(payload);
  }
}

@Module({
  providers: [
    TotalsConsumer,
    EnhancedConsumer,
    SilentConsumer,
    DoublingInterceptor,
  ],
})
class TotalsModule {}

async function bootstrap(
  imports: Parameters<typeof Test.createTestingModule>[0]['imports'],
): Promise<{
  close: () => Promise<void>;
  get: <T>(token: unknown) => T;
  broker: InMemoryKafkaBroker;
  requests: KafkaRequestReplyService;
  settle: () => Promise<void>;
}> {
  const moduleRef = await Test.createTestingModule({ imports }).compile();
  await moduleRef.init();
  const get = <T,>(token: unknown): T =>
    moduleRef.get<T>(token as never, { strict: false });
  const broker = get<InMemoryKafkaBroker>(KAFKA_TEST_BROKER);

  return {
    close: () => moduleRef.close(),
    get,
    broker,
    requests: get<KafkaRequestReplyService>(KafkaRequestReplyService),
    // `idle()` returns as soon as the broker is quiet. Draining twice crosses
    // two macrotask boundaries, so a request whose promise chain is still
    // resolving has always reached the produce by the time this returns —
    // deterministic, and no sleeps.
    settle: async () => {
      await broker.idle();
      await broker.idle();
    },
  };
}

function replies(broker: InMemoryKafkaBroker): KafkaProducerMessage[] {
  // The readiness sentinel lives on the same topic; it is not a reply.
  return broker
    .getSentTo(REPLY_TOPIC)
    .filter(message => message.headers?.[KAFKA_READINESS_PROBE_HEADER] === undefined);
}

function headerText(
  headers: KafkaMessageHeaders | undefined,
  key: string,
): string | undefined {
  const value = headers?.[key];
  return value === undefined ? undefined : String(value);
}

function correlationIdOf(
  broker: InMemoryKafkaBroker,
  topic: string,
): string {
  const [request] = broker.getSentTo(topic);
  const id = headerText(request?.headers, KEYS.correlationId);
  assert.ok(id, `no correlation id was stamped on the request to "${topic}"`);
  return id;
}

describe('request-reply', () => {
  it('resolves a request with the replying handler return value', async () => {
    const app = await bootstrap([
      KafkaTestModule.forRoot({ requestReply: { replyTopic: REPLY_TOPIC } }),
      TotalsModule,
    ]);

    const reply = await app.requests.request<{ total: number }>({
      topic: 'orders.total',
      message: { value: JSON.stringify({ customerId: 'abcd' }) },
    });

    assert.deepEqual(reply.value, { total: 4 });
    assert.equal(reply.topic, REPLY_TOPIC);
    assert.equal(reply.partition, 0);
    assert.equal(typeof reply.correlationId, 'string');
    assert.equal(reply.offset, '0');
    assert.deepEqual(app.get<TotalsConsumer>(TotalsConsumer).seen, ['abcd']);

    await app.close();
  });

  it('stamps the address on the request and never a reply partition', async () => {
    const app = await bootstrap([
      KafkaTestModule.forRoot({ requestReply: { replyTopic: REPLY_TOPIC } }),
      TotalsModule,
    ]);

    await app.requests.request({
      topic: 'orders.total',
      message: {
        key: 'customer-1',
        value: JSON.stringify({ customerId: 'ab' }),
        headers: { tenant: 'acme' },
      },
    });

    const [request] = app.broker.getSentTo('orders.total');
    assert.equal(request.key, 'customer-1');
    assert.equal(headerText(request.headers, 'tenant'), 'acme');
    assert.equal(headerText(request.headers, KEYS.replyTopic), REPLY_TOPIC);
    assert.ok(headerText(request.headers, KEYS.correlationId));
    // Deliberate: this instance consumes every partition of its reply topic, so
    // targeting one buys nothing — and `ServerKafka` treats the missing header
    // as "no partition targeting", which is exactly what we want from it.
    assert.equal(headerText(request.headers, KEYS.replyPartition), undefined);

    await app.close();
  });

  it('keys the reply by correlation id and marks it complete', async () => {
    const app = await bootstrap([
      KafkaTestModule.forRoot({ requestReply: { replyTopic: REPLY_TOPIC } }),
      TotalsModule,
    ]);

    const reply = await app.requests.request({
      topic: 'orders.total',
      message: { value: JSON.stringify({ customerId: 'ab' }) },
    });

    const [sent] = replies(app.broker);
    assert.equal(sent.key, reply.correlationId);
    assert.equal(
      headerText(sent.headers, KEYS.correlationId),
      reply.correlationId,
    );
    // Presence completes an un-migrated ClientKafka's observable.
    assert.notEqual(sent.headers?.[KEYS.disposed], undefined);
    // Request headers are not echoed: tracing propagation is an interceptor's
    // job, per the package's header neutrality.
    assert.equal(headerText(sent.headers, KEYS.replyTopic), undefined);

    await app.close();
  });

  it('replies with the post-enhancer value an interceptor produced', async () => {
    const app = await bootstrap([
      KafkaTestModule.forRoot({ requestReply: { replyTopic: REPLY_TOPIC } }),
      TotalsModule,
    ]);

    const reply = await app.requests.request({
      topic: 'orders.intercepted',
      message: { value: null },
    });

    assert.deepEqual(reply.value, { total: 1, intercepted: true });

    await app.close();
  });

  it('replies with the value an exception filter returned', async () => {
    const app = await bootstrap([
      KafkaTestModule.forRoot({ requestReply: { replyTopic: REPLY_TOPIC } }),
      TotalsModule,
    ]);

    const reply = await app.requests.request({
      topic: 'orders.filtered',
      message: { value: null },
    });

    assert.deepEqual(reply.value, { handled: 'caught by the filter' });

    await app.close();
  });

  it('passes a string through and turns undefined into a tombstone', async () => {
    const app = await bootstrap([
      KafkaTestModule.forRoot({ requestReply: { replyTopic: REPLY_TOPIC } }),
      TotalsModule,
    ]);

    const text = await app.requests.request({
      topic: 'orders.name',
      message: { value: null },
    });
    const empty = await app.requests.request({
      topic: 'orders.void',
      message: { value: null },
    });

    assert.equal(text.value, 'plain text');
    assert.equal(empty.value, null);

    await app.close();
  });

  it('rejects with the remote error when the handler error commits', async () => {
    const app = await bootstrap([
      KafkaTestModule.forRoot({ requestReply: { replyTopic: REPLY_TOPIC } }),
      TotalsModule,
    ]);

    const error = await app.requests
      .request({ topic: 'orders.rejected', message: { value: null } })
      .then(
        () => undefined,
        (thrown: unknown) => thrown,
      );

    assert.ok(error instanceof KafkaReplyRemoteError);
    assert.deepEqual(error.remote, {
      name: 'BadRequestException',
      message: 'unknown customer',
    });
    assert.equal(error.reply.value, null);

    await app.close();
  });

  it('sends no reply when the handler error maps to retry, and a redelivery still answers', async () => {
    const app = await bootstrap([
      KafkaTestModule.forRoot({
        requestReply: { replyTopic: REPLY_TOPIC, timeoutMs: 5000 },
      }),
      TotalsModule,
    ]);

    const pending = app.requests.request({
      topic: 'orders.flaky',
      message: { value: JSON.stringify({ customerId: 'x1' }) },
    });
    await app.settle();

    // The first attempt threw and mapped to 'retry': no reply exists to send.
    assert.deepEqual(replies(app.broker), []);

    // Redelivery is the broker's job, so the test plays the broker: the same
    // request message, correlation id and all, is delivered again.
    const [request] = app.broker.getSentTo('orders.flaky');
    await app.broker.emit('orders.flaky', request);

    assert.equal((await pending).value, 'recovered');

    await app.close();
  });

  it('times out with an unknown-outcome error when nobody answers', async () => {
    const app = await bootstrap([
      KafkaTestModule.forRoot({
        requestReply: { replyTopic: REPLY_TOPIC, timeoutMs: 5 },
      }),
      TotalsModule,
    ]);

    const error = await app.requests
      .request({ topic: 'orders.unanswered', message: { value: null } })
      .then(
        () => undefined,
        (thrown: unknown) => thrown,
      );

    assert.ok(error instanceof KafkaReplyTimeoutError);
    assert.equal(error.topic, 'orders.unanswered');
    assert.equal(error.timeoutMs, 5);

    await app.close();
  });

  it('honours a per-call timeout override', async () => {
    const app = await bootstrap([
      KafkaTestModule.forRoot({
        requestReply: { replyTopic: REPLY_TOPIC, timeoutMs: 60000 },
      }),
      TotalsModule,
    ]);

    await assert.rejects(
      app.requests.request(
        { topic: 'orders.unanswered', message: { value: null } },
        { timeoutMs: 5 },
      ),
      (error: unknown) =>
        error instanceof KafkaReplyTimeoutError && error.timeoutMs === 5,
    );

    await app.close();
  });

  it('drops a late reply that arrives after the timeout', async () => {
    const app = await bootstrap([
      KafkaTestModule.forRoot({
        requestReply: { replyTopic: REPLY_TOPIC, timeoutMs: 5 },
      }),
      TotalsModule,
    ]);

    await assert.rejects(
      app.requests.request({ topic: 'orders.unanswered', message: { value: null } }),
      KafkaReplyTimeoutError,
    );

    // The correlation id is no longer claimed, so the reply lands on a map miss
    // exactly like another instance's reply would.
    const correlationId = correlationIdOf(app.broker, 'orders.unanswered');
    await app.broker.emit(REPLY_TOPIC, {
      value: JSON.stringify({ total: 1 }),
      headers: { [KEYS.correlationId]: correlationId },
    });
    await app.settle();

    await app.close();
  });

  it('drops a reply for another instance and still resolves its own', async () => {
    const app = await bootstrap([
      KafkaTestModule.forRoot({
        requestReply: { replyTopic: REPLY_TOPIC, timeoutMs: 5000 },
      }),
      TotalsModule,
    ]);

    const pending = app.requests.request({
      topic: 'orders.unanswered',
      message: { value: null },
    });
    await app.settle();

    // The hot path of the chosen routing: (N-1)/N of everything this consumer
    // fetches belongs to another instance. It is dropped silently.
    await app.broker.emit(REPLY_TOPIC, {
      value: '"someone else"',
      headers: { [KEYS.correlationId]: 'a-correlation-id-we-never-issued' },
    });
    // Neither a message with no headers at all nor one whose headers carry no
    // correlation id is a reply. The reply topic is shared infrastructure, and
    // unrelated traffic on it must not disturb a pending request.
    await app.broker.emit(REPLY_TOPIC, { value: '"headerless noise"' });
    await app.broker.emit(REPLY_TOPIC, {
      value: '"uncorrelated noise"',
      headers: { trace: 'not-a-correlation-id' },
    });

    const correlationId = correlationIdOf(app.broker, 'orders.unanswered');
    await app.broker.emit(REPLY_TOPIC, {
      value: '"ours"',
      headers: { [KEYS.correlationId]: correlationId },
    });

    assert.equal((await pending).value, 'ours');

    await app.close();
  });

  it('settles once and drops a duplicate reply', async () => {
    const app = await bootstrap([
      KafkaTestModule.forRoot({
        requestReply: { replyTopic: REPLY_TOPIC, timeoutMs: 5000 },
      }),
      TotalsModule,
    ]);

    const pending = app.requests.request({
      topic: 'orders.unanswered',
      message: { value: null },
    });
    await app.settle();

    const correlationId = correlationIdOf(app.broker, 'orders.unanswered');
    const reply = {
      headers: { [KEYS.correlationId]: correlationId },
    };
    await app.broker.emit(REPLY_TOPIC, { ...reply, value: '"first"' });
    await app.broker.emit(REPLY_TOPIC, { ...reply, value: '"second"' });
    await app.settle();

    // At-most-once resolution is the only coherent promise semantics.
    assert.equal((await pending).value, 'first');

    await app.close();
  });

  it('reports the coordinates a reply was consumed from', async () => {
    const app = await bootstrap([
      KafkaTestModule.forRoot({
        requestReply: { replyTopic: REPLY_TOPIC, timeoutMs: 5000 },
      }),
      TotalsModule,
    ]);

    const pending = app.requests.request({
      topic: 'orders.unanswered',
      message: { value: null },
    });
    await app.settle();

    const correlationId = correlationIdOf(app.broker, 'orders.unanswered');
    await app.broker.emit(REPLY_TOPIC, {
      partition: 2,
      value: '"targeted"',
      headers: { [KEYS.correlationId]: correlationId, trace: 'abc' },
    });

    const reply = await pending;
    assert.equal(reply.partition, 2);
    assert.equal(headerText(reply.headers, 'trace'), 'abc');

    await app.close();
  });

  it('aborts the wait when the caller signals, without cancelling the remote work', async () => {
    const app = await bootstrap([
      KafkaTestModule.forRoot({
        requestReply: { replyTopic: REPLY_TOPIC, timeoutMs: 60000 },
      }),
      TotalsModule,
    ]);

    const controller = new AbortController();
    const pending = app.requests.request(
      { topic: 'orders.unanswered', message: { value: null } },
      { signal: controller.signal },
    );
    await app.settle();
    controller.abort();

    await assert.rejects(pending, KafkaReplyAbortedError);
    // The request really was produced: aborting cancels the wait, not the work.
    assert.equal(app.broker.getSentTo('orders.unanswered').length, 1);

    await app.close();
  });

  it('rejects with the caller reason for an already-aborted signal', async () => {
    const app = await bootstrap([
      KafkaTestModule.forRoot({ requestReply: { replyTopic: REPLY_TOPIC } }),
      TotalsModule,
    ]);

    const controller = new AbortController();
    const reason = new Error('caller gave up first');
    controller.abort(reason);

    await assert.rejects(
      app.requests.request(
        { topic: 'orders.total', message: { value: null } },
        { signal: controller.signal },
      ),
      (error: unknown) => error === reason,
    );
    // Nothing was produced: the wait never got past readiness.
    assert.deepEqual(app.broker.getSentTo('orders.total'), []);

    await app.close();
  });

  it('fails pending and new requests at shutdown instead of draining them', async () => {
    const app = await bootstrap([
      KafkaTestModule.forRoot({
        requestReply: { replyTopic: REPLY_TOPIC, timeoutMs: 60000 },
      }),
      TotalsModule,
    ]);

    const pending = app.requests.request({
      topic: 'orders.unanswered',
      message: { value: null },
    });
    await app.settle();

    await app.close();

    await assert.rejects(pending, KafkaReplyAbortedError);
    await assert.rejects(
      app.requests.request({ topic: 'orders.total', message: { value: null } }),
      /shutting down/,
    );
  });

  it('rejects request() when requestReply was never configured', async () => {
    const app = await bootstrap([KafkaTestModule.forRoot(), TotalsModule]);

    await assert.rejects(
      app.requests.request({ topic: 'orders.total', message: { value: null } }),
      /request-reply is not configured/,
    );
    // Inert means inert: no reply consumer, no readiness sentinel, no topic.
    assert.deepEqual(app.broker.getSentTo(REPLY_TOPIC), []);

    await app.close();
  });

  it('rejects a request header that collides with a reserved key', async () => {
    const app = await bootstrap([
      KafkaTestModule.forRoot({ requestReply: { replyTopic: REPLY_TOPIC } }),
      TotalsModule,
    ]);

    await assert.rejects(
      app.requests.request({
        topic: 'orders.total',
        message: { value: null, headers: { [KEYS.correlationId]: 'mine' } },
      }),
      /is reserved by request-reply/,
    );

    await app.close();
  });

  it('runs a replying handler normally when the message has no reply address', async () => {
    const app = await bootstrap([
      KafkaTestModule.forRoot({ requestReply: { replyTopic: REPLY_TOPIC } }),
      TotalsModule,
    ]);

    await app.broker.emit('orders.total', {
      value: JSON.stringify({ customerId: 'replayed' }),
    });
    await app.settle();

    // Mixed traffic on a request topic is legitimate: the handler ran, and only
    // the reply step was skipped.
    assert.deepEqual(app.get<TotalsConsumer>(TotalsConsumer).seen, ['replayed']);
    assert.deepEqual(replies(app.broker), []);

    await app.close();
  });

  it('leaves fire-and-forget handlers alone on a topic that also carries requests', async () => {
    const app = await bootstrap([
      KafkaTestModule.forRoot({ requestReply: { replyTopic: REPLY_TOPIC } }),
      TotalsModule,
    ]);

    await app.broker.emit('orders.silent', {
      value: '"hello"',
      headers: {
        [KEYS.correlationId]: 'c1',
        [KEYS.replyTopic]: REPLY_TOPIC,
      },
    });
    await app.settle();

    assert.deepEqual(app.get<SilentConsumer>(SilentConsumer).seen, ['hello']);
    // A reply address is never an ambient instruction to produce: without
    // `reply: true` the handler stays fire-and-forget.
    assert.deepEqual(replies(app.broker), []);

    await app.close();
  });

  it('produces a readiness sentinel onto its own reply topic before requesting', async () => {
    const app = await bootstrap([
      KafkaTestModule.forRoot({ requestReply: { replyTopic: REPLY_TOPIC } }),
      TotalsModule,
    ]);
    await app.settle();

    const sentinels = app.broker
      .getSentTo(REPLY_TOPIC)
      .filter(
        message => message.headers?.[KAFKA_READINESS_PROBE_HEADER] !== undefined,
      );

    assert.equal(sentinels.length, 1);
    assert.equal(sentinels[0].value, null);
    assert.match(
      String(sentinels[0].headers?.[KAFKA_READINESS_PROBE_HEADER]),
      new RegExp(`^${REPLY_TOPIC}-`),
    );

    await app.close();
  });
});

describe('request-reply interop with an un-migrated ClientKafka', () => {
  it('answers on the exact topic and partition the request advertised', async () => {
    const app = await bootstrap([
      KafkaTestModule.forRoot({ requestReply: { replyTopic: REPLY_TOPIC } }),
      TotalsModule,
    ]);

    // Exactly what `ClientKafka.send()` stamps: it always advertises a
    // partition, because it throws client-side without one it owns.
    await app.broker.emit('orders.total', {
      value: JSON.stringify({ customerId: 'abc' }),
      headers: {
        [KEYS.correlationId]: 'legacy-1',
        [KEYS.replyTopic]: 'orders.total.reply',
        [KEYS.replyPartition]: '2',
      },
    });
    await app.settle();

    const [reply] = app.broker.getSentTo('orders.total.reply');
    assert.equal(reply.partition, 2);
    assert.equal(reply.key, 'legacy-1');
    assert.equal(reply.value, JSON.stringify({ total: 3 }));
    assert.equal(headerText(reply.headers, KEYS.correlationId), 'legacy-1');

    await app.close();
  });

  it('skips and commits a request whose reply partition is unparseable', async () => {
    const app = await bootstrap([
      KafkaTestModule.forRoot({ requestReply: { replyTopic: REPLY_TOPIC } }),
      TotalsModule,
    ]);

    await app.broker.emit('orders.total', {
      value: JSON.stringify({ customerId: 'abc' }),
      headers: {
        [KEYS.correlationId]: 'legacy-2',
        [KEYS.replyTopic]: 'orders.total.reply',
        [KEYS.replyPartition]: 'not-a-partition',
      },
    });
    await app.settle();

    // Statically undeliverable: retrying could never succeed, and would block
    // the partition behind a poison request.
    assert.deepEqual(app.broker.getSentTo('orders.total.reply'), []);
    assert.deepEqual(app.get<TotalsConsumer>(TotalsConsumer).seen, ['abc']);

    await app.close();
  });

  it('reads renamed header keys on both sides', async () => {
    const headers = { correlationId: 'x-correlation-id', replyTopic: 'x-reply-to' };
    const app = await bootstrap([
      KafkaTestModule.forRoot({
        requestReply: { replyTopic: REPLY_TOPIC, headers },
      }),
      TotalsModule,
    ]);

    const reply = await app.requests.request({
      topic: 'orders.total',
      message: { value: JSON.stringify({ customerId: 'ab' }) },
    });

    assert.deepEqual(reply.value, { total: 2 });
    const [request] = app.broker.getSentTo('orders.total');
    assert.equal(headerText(request.headers, 'x-reply-to'), REPLY_TOPIC);
    assert.equal(headerText(request.headers, KEYS.correlationId), undefined);

    await app.close();
  });
});

describe('request-reply bootstrap validation', () => {
  it('refuses a handler that is both batch and reply', async () => {
    @Injectable()
    @KafkaConsumer('orders.batched', { groupId: 'batched' })
    class BatchReplyConsumer {
      @KafkaHandler(undefined, { batch: true, reply: true })
      handle(): void {}
    }

    @Module({ providers: [BatchReplyConsumer] })
    class BatchReplyModule {}

    await assert.rejects(
      bootstrap([KafkaTestModule.forRoot(), BatchReplyModule]),
      /both "batch: true" and "reply: true"/,
    );
  });

  it('refuses two replying handlers on one topic', async () => {
    @Injectable()
    @KafkaConsumer('orders.contested', { groupId: 'first' })
    class FirstReplier {
      @KafkaHandler(undefined, { reply: true })
      handle(): void {}
    }

    @Injectable()
    @KafkaConsumer('orders.contested', { groupId: 'second' })
    class SecondReplier {
      @KafkaHandler(undefined, { reply: true })
      handle(): void {}
    }

    @Module({ providers: [FirstReplier, SecondReplier] })
    class ContestedModule {}

    await assert.rejects(
      bootstrap([KafkaTestModule.forRoot(), ContestedModule]),
      /both declare "reply: true" for topic "orders.contested"/,
    );
  });
});

describe('request-reply delivery failures', () => {
  it('maps an undeliverable reply through the error mapper', async () => {
    const broker = new InMemoryKafkaBroker();
    const mapped: unknown[] = [];
    // Wired without `KafkaTestModule` so the driver can make one topic
    // unwritable — the "reply topic missing on the replier's side" case the
    // in-memory broker has no other way to express.
    const moduleRef = await Test.createTestingModule({
      imports: [
        KafkaModule.forRoot({
          driverFactory: failingReplyDriver(broker, 'unwritable.replies'),
          errorMapper: (error): KafkaErrorBehavior => {
            mapped.push(error);
            return 'commit';
          },
        }),
        TotalsModule,
      ],
    }).compile();
    await moduleRef.init();

    await broker.emit('orders.total', {
      value: JSON.stringify({ customerId: 'abc' }),
      headers: {
        [KEYS.correlationId]: 'c1',
        [KEYS.replyTopic]: 'unwritable.replies',
      },
    });
    await broker.idle();

    const [error] = mapped;
    assert.ok(error instanceof KafkaReplyDeliveryError);
    assert.equal(error.topic, 'unwritable.replies');
    assert.equal(error.correlationId, 'c1');
    assert.match(String((error as { cause?: Error }).cause?.message), /no ACL/);
    // 'commit' means the message is acknowledged: the replier does not spin on
    // an address it can never write to.
    assert.deepEqual(
      moduleRef.get(TotalsConsumer, { strict: false }).seen,
      ['abc'],
    );

    await moduleRef.close();
  });
});

/**
 * A driver that behaves exactly like the in-memory broker's except that writing
 * to one topic fails.
 */
function failingReplyDriver(
  broker: InMemoryKafkaBroker,
  unwritableTopic: string,
): () => ReturnType<InMemoryKafkaBroker['createDriver']> {
  return () => {
    const driver = broker.createDriver();
    return {
      ...driver,
      createProducer: () => {
        const producer = driver.createProducer();
        return {
          ...producer,
          send: record =>
            record.topic === unwritableTopic
              ? Promise.reject(new Error(`no ACL for "${record.topic}"`))
              : producer.send(record),
        };
      },
    };
  };
}

@Injectable()
@KafkaConsumer('orders.contexts', { groupId: 'contexts' })
class ContextConsumer {
  readonly topics: string[] = [];

  @KafkaHandler(undefined, { reply: true })
  handle(@KafkaCtx() context: KafkaContext): string {
    this.topics.push(context.getTopic());
    return 'ok';
  }
}

@Module({ providers: [ContextConsumer] })
class ContextModule {}

describe('request-reply handler context', () => {
  it('gives a replying handler the same context a plain handler gets', async () => {
    const app = await bootstrap([
      KafkaTestModule.forRoot({ requestReply: { replyTopic: REPLY_TOPIC } }),
      ContextModule,
    ]);

    const reply = await app.requests.request({
      topic: 'orders.contexts',
      message: { value: null },
    });

    assert.equal(reply.value, 'ok');
    assert.deepEqual(
      app.get<ContextConsumer>(ContextConsumer).topics,
      ['orders.contexts'],
    );

    await app.close();
  });
});
