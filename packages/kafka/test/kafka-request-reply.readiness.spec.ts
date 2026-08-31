import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { setTimeout as delay } from 'node:timers/promises';
import {
  KafkaClientDriver,
  KafkaConsumerConfig,
  KafkaEachMessageHandler,
  KafkaProducerMessage,
  KafkaSendRecord,
  KafkaSubscription,
} from '../driver';
import { KafkaModuleOptions, KafkaRequestReplyOptions } from '../interfaces';
import { KafkaProducerService } from '../kafka-producer.service';
import { KafkaReplyTimeoutError } from '../kafka-request-reply.errors';
import { KafkaRequestReplyService } from '../kafka-request-reply.service';
import { DEFAULT_KAFKA_REQUEST_REPLY_HEADERS as KEYS } from '../kafka-request-reply.protocol';

const REPLY_TOPIC = 'app.replies';

/**
 * Poll `count` until it stops moving across a window, and return where it
 * settled. The readiness probe re-produces its sentinel on a cadence, so
 * "production stopped" is only observable as "the count is no longer climbing" —
 * polled against an observable condition with a bound, never slept on.
 */
async function quiesce(count: () => number, quietMs: number): Promise<number> {
  const deadline = Date.now() + 2_000;
  for (;;) {
    const before = count();
    await delay(quietMs);
    if (count() === before) {
      return before;
    }
    if (Date.now() > deadline) {
      throw new Error(`Sentinel production never stopped (${count()} so far)`);
    }
  }
}

/**
 * These tests drive the service with a hand-built driver instead of the
 * in-memory broker, because the properties under test are precisely the ones a
 * cooperating broker hides: a reply consumer that never becomes ready, and a
 * reply topic that cannot be written to. Both are the difference between a
 * named error and a silent hang.
 */
interface Harness {
  service: KafkaRequestReplyService;
  configs: KafkaConsumerConfig[];
  subscriptions: KafkaSubscription[];
  sent: KafkaSendRecord[];
  disconnects: () => number;
  /** Make every produce fail until cleared — an unwritable reply topic. */
  setSendFailure: (error?: Error) => void;
}

function harness(
  requestReply: KafkaRequestReplyOptions,
  options: { echo?: boolean } = {},
): Harness {
  const configs: KafkaConsumerConfig[] = [];
  const subscriptions: KafkaSubscription[] = [];
  const sent: KafkaSendRecord[] = [];
  let each: KafkaEachMessageHandler | undefined;
  let disconnects = 0;
  const echo = options.echo ?? true;
  let sendFailure: Error | undefined;

  const driver: KafkaClientDriver = {
    createProducer: () => {
      throw new Error('the reply consumer uses the shared producer service');
    },
    createConsumer: config => {
      configs.push(config ?? {});
      return {
        connect: async () => {},
        disconnect: async () => {
          disconnects += 1;
        },
        subscribe: async subscription => void subscriptions.push(subscription),
        run: async runConfig => {
          each = runConfig.eachMessage;
        },
      };
    },
  };

  const deliver = async (message: KafkaProducerMessage): Promise<void> => {
    await each?.({
      topic: REPLY_TOPIC,
      partition: message.partition ?? 0,
      message: { value: message.value, headers: message.headers, offset: '0' },
    });
  };

  const producer = {
    send: async (record: KafkaSendRecord) => {
      if (sendFailure) {
        throw sendFailure;
      }
      sent.push(record);
      if (echo && record.topic === REPLY_TOPIC) {
        await deliver(record.messages[0]);
      }
      return [];
    },
  } as unknown as KafkaProducerService;

  const moduleOptions: KafkaModuleOptions = { requestReply };

  return {
    service: new KafkaRequestReplyService(moduleOptions, driver, producer),
    configs,
    subscriptions,
    sent,
    disconnects: () => disconnects,
    setSendFailure: error => {
      sendFailure = error;
    },
  };
}

describe('reply-consumer topology', () => {
  it('gives every instance its own single-member group', async () => {
    const first = harness({ replyTopic: REPLY_TOPIC });
    const second = harness({ replyTopic: REPLY_TOPIC });

    await first.service.onApplicationBootstrap();
    await second.service.onApplicationBootstrap();

    const [firstGroup] = first.configs.map(config => config.groupId);
    const [secondGroup] = second.configs.map(config => config.groupId);

    // Nothing to rebalance is the whole correctness argument: a group of one
    // cannot have a reply moved away from it by another instance's membership
    // change.
    assert.match(String(firstGroup), new RegExp(`^${REPLY_TOPIC}-`));
    assert.notEqual(firstGroup, secondGroup);

    await first.service.onApplicationShutdown();
    await second.service.onApplicationShutdown();
  });

  it('starts from latest and commits no offsets', async () => {
    const app = harness({ replyTopic: REPLY_TOPIC });

    await app.service.onApplicationBootstrap();

    // "From latest" is declared at consumer creation, not on `subscribe()`.
    // Confluent's compatibility layer rejects `fromBeginning` as a subscribe
    // option with `ERR__INVALID_ARG`, which no in-memory broker can reproduce —
    // the real-broker suite caught it, and this assertion is what keeps it
    // caught.
    assert.deepEqual(app.subscriptions, [{ topics: [REPLY_TOPIC] }]);
    assert.equal(app.configs[0].fromBeginning, false);
    // Committed offsets for a group that dies with the process are pure
    // `__consumer_offsets` churn; the correlation map is the source of truth.
    assert.equal(app.configs[0]['enable.auto.commit'], false);

    await app.service.onApplicationShutdown();
  });

  it('forwards advanced consumer config but never a shared group id', async () => {
    const app = harness({
      replyTopic: REPLY_TOPIC,
      groupIdPrefix: 'replies-',
      consumer: {
        groupId: 'a-shared-group',
        'enable.auto.commit': true,
        'fetch.min.bytes': 1,
      },
    });

    await app.service.onApplicationBootstrap();

    const [config] = app.configs;
    assert.equal(config['fetch.min.bytes'], 1);
    // The caller may override the commit behaviour...
    assert.equal(config['enable.auto.commit'], true);
    // ...but never the group id, which is the routing strategy itself.
    assert.notEqual(config.groupId, 'a-shared-group');
    assert.match(String(config.groupId), /^replies-/);

    await app.service.onApplicationShutdown();
  });

  it('disconnects the reply consumer at shutdown', async () => {
    const app = harness({ replyTopic: REPLY_TOPIC });

    await app.service.onApplicationBootstrap();
    assert.equal(app.disconnects(), 0);

    await app.service.onApplicationShutdown();
    assert.equal(app.disconnects(), 1);
  });

  it('proves readiness with one valueless sentinel on its own reply topic', async () => {
    const app = harness({ replyTopic: REPLY_TOPIC });

    await app.service.onApplicationBootstrap();
    await assert.rejects(
      app.service.request(
        { topic: 'orders.total', message: { value: null } },
        { timeoutMs: 5 },
      ),
      KafkaReplyTimeoutError,
    );

    const [sentinel, request] = app.sent;
    assert.equal(sentinel.topic, REPLY_TOPIC);
    assert.equal(sentinel.messages[0].value, null);
    // The sentinel rides the same correlation machinery a real reply does,
    // which is why the fan-out path needs no special case for it.
    assert.ok(sentinel.messages[0].headers?.[KEYS.correlationId]);
    assert.equal(request.topic, 'orders.total');

    await app.service.onApplicationShutdown();
  });
});

describe('reply-consumer readiness failures', () => {
  it('fails fast and names the reply topic when the consumer never fetches', async () => {
    const app = harness(
      { replyTopic: REPLY_TOPIC, readinessTimeoutMs: 5 },
      { echo: false },
    );

    await app.service.onApplicationBootstrap();

    await assert.rejects(
      app.service.request({ topic: 'orders.total', message: { value: null } }),
      (error: unknown) => {
        // The worst diagnostic is a silent hang; the best is an error with the
        // topic in it.
        assert.match(String((error as Error).message), /was not ready/);
        assert.match(String((error as Error).message), new RegExp(REPLY_TOPIC));
        return true;
      },
    );
    // No request was produced: a reply landing before the consumer's position
    // is established would be skipped forever.
    assert.deepEqual(
      app.sent.filter(record => record.topic === 'orders.total'),
      [],
    );

    await app.service.onApplicationShutdown();
  });

  it('surfaces an unwritable reply topic as a readiness failure, cause attached', async () => {
    const app = harness({ replyTopic: REPLY_TOPIC });
    const cause = new Error('TOPIC_AUTHORIZATION_FAILED');
    app.setSendFailure(cause);

    await app.service.onApplicationBootstrap();

    await assert.rejects(
      app.service.request({ topic: 'orders.total', message: { value: null } }),
      (error: unknown) => {
        assert.match(String((error as Error).message), /could not be written to/);
        assert.equal((error as { cause?: unknown }).cause, cause);
        return true;
      },
    );

    await app.service.onApplicationShutdown();
  });

  it('re-arms the probe so a transient failure does not poison the process', async () => {
    const app = harness({ replyTopic: REPLY_TOPIC });
    app.setSendFailure(new Error('broker blip at boot'));

    await app.service.onApplicationBootstrap();
    await assert.rejects(
      app.service.request({ topic: 'orders.total', message: { value: null } }),
      /could not be written to/,
    );

    // The blip passes. The next request probes again rather than reusing the
    // failed one, so readiness is reached and the request itself is produced.
    app.setSendFailure(undefined);
    await assert.rejects(
      app.service.request(
        { topic: 'orders.total', message: { value: null } },
        { timeoutMs: 5 },
      ),
      KafkaReplyTimeoutError,
    );
    assert.equal(
      app.sent.filter(record => record.topic === 'orders.total').length,
      1,
    );

    await app.service.onApplicationShutdown();
  });

  it('stops re-producing sentinels once every caller has given up', async () => {
    const readinessTimeoutMs = 40;
    const app = harness(
      { replyTopic: REPLY_TOPIC, readinessTimeoutMs },
      { echo: false },
    );
    const sentinels = (): number =>
      app.sent.filter(record => record.topic === REPLY_TOPIC).length;

    await app.service.onApplicationBootstrap();
    await assert.rejects(
      app.service.request({ topic: 'orders.total', message: { value: null } }),
      /was not ready/,
    );

    // One sentinel is not enough against a real broker, so the probe re-produces
    // — but only while somebody is still waiting. Once the budget is spent the
    // loop stops at its next check instead of producing for the life of the
    // process.
    const produced = await quiesce(sentinels, readinessTimeoutMs);
    assert.equal(
      produced > 1,
      true,
      `the probe should re-produce its sentinel while a caller waits, sent ${produced}`,
    );

    // And the next request arms a fresh probe rather than inheriting the
    // abandoned one: a consumer that was not fetching a moment ago may be now.
    await assert.rejects(
      app.service.request({ topic: 'orders.total', message: { value: null } }),
      /was not ready/,
    );
    assert.equal(sentinels() > produced, true, 'a fresh probe was armed');

    await app.service.onApplicationShutdown();
  });

  it('reuses one probe instead of sending a sentinel per request', async () => {
    const app = harness({ replyTopic: REPLY_TOPIC });

    await app.service.onApplicationBootstrap();
    await Promise.allSettled([
      app.service.request(
        { topic: 'orders.total', message: { value: null } },
        { timeoutMs: 5 },
      ),
      app.service.request(
        { topic: 'orders.total', message: { value: null } },
        { timeoutMs: 5 },
      ),
    ]);

    // A probe is shared by every concurrent caller: readiness is a property of
    // the instance, not of one request, so two requests do not start two of
    // them.
    assert.equal(
      app.sent.filter(record => record.topic === REPLY_TOPIC).length,
      1,
    );

    await app.service.onApplicationShutdown();
  });
});
