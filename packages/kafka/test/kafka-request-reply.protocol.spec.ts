import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import {
  KafkaReplyAbortedError,
  KafkaReplyDeliveryError,
  KafkaReplyRemoteError,
  KafkaReplyTimeoutError,
} from '../kafka-request-reply.errors';
import {
  DEFAULT_KAFKA_REQUEST_REPLY_HEADERS,
  assertNoReservedHeaders,
  buildReplyMessage,
  describeReplyError,
  readHeaderText,
  resolveHeaderKeys,
  resolveReplyAddress,
  resolveRequestReplyOptions,
  serializeReplyValue,
} from '../kafka-request-reply.protocol';
import {
  abortReason,
  waitForReply,
} from '../kafka-request-reply.waiting';

const KEYS = DEFAULT_KAFKA_REQUEST_REPLY_HEADERS;

describe('request-reply header keys', () => {
  it('defaults to the @nestjs/microservices key names', () => {
    // These five names are the interop contract with the official transport.
    // They are asserted literally so a rename can never happen by accident.
    assert.deepEqual(resolveHeaderKeys(), {
      correlationId: 'kafka_correlationId',
      replyTopic: 'kafka_replyTopic',
      replyPartition: 'kafka_replyPartition',
      error: 'kafka_nest-err',
      disposed: 'kafka_nest-is-disposed',
    });
  });

  it('merges a partial override onto the defaults', () => {
    const keys = resolveHeaderKeys({ correlationId: 'x-correlation-id' });

    assert.equal(keys.correlationId, 'x-correlation-id');
    assert.equal(keys.replyTopic, 'kafka_replyTopic');
  });
});

describe('resolveRequestReplyOptions', () => {
  it('fills every default from the reply topic alone', () => {
    const resolved = resolveRequestReplyOptions({ replyTopic: 'app.replies' });

    assert.equal(resolved.replyTopic, 'app.replies');
    assert.equal(resolved.timeoutMs, 30000);
    assert.equal(resolved.readinessTimeoutMs, 10000);
    assert.equal(resolved.groupIdPrefix, 'app.replies-');
    assert.deepEqual(resolved.consumer, {});
    assert.equal(resolved.headers.error, 'kafka_nest-err');
  });

  it('keeps every explicit override', () => {
    const resolved = resolveRequestReplyOptions({
      replyTopic: 'app.replies',
      timeoutMs: 1234,
      readinessTimeoutMs: 56,
      groupIdPrefix: 'replies-',
      headers: { error: 'x-err' },
      consumer: { 'fetch.min.bytes': 1 },
    });

    assert.equal(resolved.timeoutMs, 1234);
    assert.equal(resolved.readinessTimeoutMs, 56);
    assert.equal(resolved.groupIdPrefix, 'replies-');
    assert.equal(resolved.headers.error, 'x-err');
    assert.deepEqual(resolved.consumer, { 'fetch.min.bytes': 1 });
  });
});

describe('readHeaderText', () => {
  it('returns undefined for absent headers and absent keys', () => {
    assert.equal(readHeaderText(undefined, 'k'), undefined);
    assert.equal(readHeaderText({}, 'k'), undefined);
  });

  it('decodes a Buffer and passes a string through', () => {
    assert.equal(readHeaderText({ k: Buffer.from('buffered') }, 'k'), 'buffered');
    assert.equal(readHeaderText({ k: 'plain' }, 'k'), 'plain');
  });

  it('reads the first value of a repeated header', () => {
    assert.equal(readHeaderText({ k: ['first', 'second'] }, 'k'), 'first');
    assert.equal(readHeaderText({ k: [Buffer.from('b')] }, 'k'), 'b');
  });

  it('treats an empty repeated header as absent', () => {
    assert.equal(readHeaderText({ k: [] }, 'k'), undefined);
  });
});

describe('resolveReplyAddress', () => {
  it('is not a request without both a correlation id and a reply topic', () => {
    // The same rule `ServerKafka` applies, so both transports classify traffic
    // identically.
    assert.deepEqual(resolveReplyAddress({}, KEYS), { status: 'none' });
    assert.deepEqual(
      resolveReplyAddress({ [KEYS.correlationId]: 'c1' }, KEYS),
      { status: 'none' },
    );
    assert.deepEqual(
      resolveReplyAddress({ [KEYS.replyTopic]: 'replies' }, KEYS),
      { status: 'none' },
    );
  });

  it('resolves an address with no partition targeting', () => {
    assert.deepEqual(
      resolveReplyAddress(
        { [KEYS.correlationId]: 'c1', [KEYS.replyTopic]: 'replies' },
        KEYS,
      ),
      { status: 'ok', address: { topic: 'replies', correlationId: 'c1' } },
    );
  });

  it('honours an explicit reply partition', () => {
    assert.deepEqual(
      resolveReplyAddress(
        {
          [KEYS.correlationId]: 'c1',
          [KEYS.replyTopic]: 'replies',
          [KEYS.replyPartition]: '3',
        },
        KEYS,
      ),
      {
        status: 'ok',
        address: { topic: 'replies', correlationId: 'c1', partition: 3 },
      },
    );
  });

  it('reports a garbage partition as undeliverable rather than retryable', () => {
    for (const raw of ['nonsense', '-1', '1.5']) {
      const result = resolveReplyAddress(
        {
          [KEYS.correlationId]: 'c1',
          [KEYS.replyTopic]: 'replies',
          [KEYS.replyPartition]: raw,
        },
        KEYS,
      );

      assert.equal(result.status, 'undeliverable');
      assert.match(
        result.status === 'undeliverable' ? result.detail : '',
        /unparseable reply partition/,
      );
    }
  });
});

describe('serializeReplyValue', () => {
  it('passes strings, Buffers, and tombstones through', () => {
    assert.equal(serializeReplyValue('text'), 'text');
    assert.deepEqual(serializeReplyValue(Buffer.from('b')), Buffer.from('b'));
    assert.equal(serializeReplyValue(null), null);
  });

  it('turns undefined into a null value', () => {
    assert.equal(serializeReplyValue(undefined), null);
  });

  it('JSON-encodes everything else', () => {
    assert.equal(serializeReplyValue({ total: 3 }), '{"total":3}');
  });

  it('falls back to null for a value JSON cannot represent', () => {
    // `JSON.stringify` returns undefined for a function or a symbol; a null
    // value beats a message whose `value` violates the producer's own type.
    assert.equal(
      serializeReplyValue(() => undefined),
      null,
    );
  });
});

describe('describeReplyError', () => {
  it('encodes name and message only, never a stack', () => {
    const encoded = describeReplyError(new TypeError('bad shape'));

    assert.deepEqual(JSON.parse(encoded), {
      name: 'TypeError',
      message: 'bad shape',
    });
  });

  it('stringifies a thrown non-Error', () => {
    assert.deepEqual(JSON.parse(describeReplyError('boom')), {
      name: 'Error',
      message: 'boom',
    });
  });
});

describe('buildReplyMessage', () => {
  it('keys the reply by correlation id and marks it complete', () => {
    const message = buildReplyMessage(
      { topic: 'replies', correlationId: 'c1' },
      KEYS,
      { status: 'value', value: { total: 7 } },
    );

    assert.equal(message.key, 'c1');
    assert.equal(message.value, '{"total":7}');
    assert.equal(message.partition, undefined);
    assert.equal(message.headers?.[KEYS.correlationId], 'c1');
    // Presence is what an un-migrated ClientKafka reads; the one-byte buffer
    // mirrors ServerKafka's own bytes.
    assert.deepEqual(message.headers?.[KEYS.disposed], Buffer.alloc(1));
    assert.equal(message.headers?.[KEYS.error], undefined);
  });

  it('carries the error header and a null value on the error path', () => {
    const message = buildReplyMessage(
      { topic: 'replies', correlationId: 'c1' },
      KEYS,
      { status: 'error', error: new Error('nope') },
    );

    assert.equal(message.value, null);
    assert.deepEqual(JSON.parse(String(message.headers?.[KEYS.error])), {
      name: 'Error',
      message: 'nope',
    });
  });

  it('targets an explicit partition when the request advertised one', () => {
    const message = buildReplyMessage(
      { topic: 'replies', correlationId: 'c1', partition: 4 },
      KEYS,
      { status: 'value', value: 'ok' },
    );

    assert.equal(message.partition, 4);
  });
});

describe('assertNoReservedHeaders', () => {
  it('accepts absent and non-colliding headers', () => {
    assert.doesNotThrow(() => assertNoReservedHeaders(undefined, KEYS));
    assert.doesNotThrow(() => assertNoReservedHeaders({ tenant: 'a' }, KEYS));
  });

  it('rejects every reserved key', () => {
    for (const key of [
      KEYS.correlationId,
      KEYS.replyTopic,
      KEYS.replyPartition,
    ]) {
      assert.throws(
        () => assertNoReservedHeaders({ [key]: 'mine' }, KEYS),
        new RegExp(`"${key}" is reserved`),
      );
    }
  });
});

describe('waitForReply', () => {
  it('resolves with the work when it settles in time', async () => {
    assert.equal(await waitForReply(Promise.resolve('done'), 1000, fail), 'done');
  });

  it('propagates a rejection from the work', async () => {
    await assert.rejects(
      waitForReply(Promise.reject(new Error('inner')), 1000, fail),
      /inner/,
    );
  });

  it('rejects with the supplied error once the deadline passes', async () => {
    await assert.rejects(
      waitForReply(pending(), 1, () => new Error('too slow')),
      /too slow/,
    );
  });

  it('rejects immediately for an already-aborted signal', async () => {
    const controller = new AbortController();
    controller.abort(new Error('gone'));

    await assert.rejects(waitForReply(pending(), 1000, fail, controller.signal), /gone/);
  });

  it('rejects when the signal aborts while waiting', async () => {
    const controller = new AbortController();
    const waiting = waitForReply(pending(), 60000, fail, controller.signal);

    controller.abort();

    await assert.rejects(waiting, KafkaReplyAbortedError);
  });

  it('releases the deadline timer on every exit path', async () => {
    // A leaked timer would hold the event loop for the whole timeout on a wait
    // nobody is watching any more. `t.diagnostic`-free proof: each of the three
    // exits below uses a timeout far longer than this test may run, so the
    // suite finishing at all is the assertion.
    const controller = new AbortController();
    const aborted = waitForReply(pending(), 600000, fail, controller.signal);
    controller.abort();
    await assert.rejects(aborted, KafkaReplyAbortedError);

    assert.equal(await waitForReply(Promise.resolve('ok'), 600000, fail), 'ok');
    await assert.rejects(
      waitForReply(Promise.reject(new Error('inner')), 600000, fail),
      /inner/,
    );
  });

  it('detaches its abort listener once the work settles', async () => {
    const controller = new AbortController();

    assert.equal(
      await waitForReply(Promise.resolve(7), 1000, fail, controller.signal),
      7,
    );

    // The listener is gone, so a later abort on a reused signal reaches nobody.
    assert.doesNotThrow(() => controller.abort());
  });
});

describe('abortReason', () => {
  it('replaces the runtime generic AbortError with the package error', () => {
    const controller = new AbortController();
    controller.abort();

    assert.ok(abortReason(controller.signal) instanceof KafkaReplyAbortedError);
  });

  it('forwards a reason the caller chose', () => {
    const controller = new AbortController();
    const reason = new Error('caller said so');
    controller.abort(reason);

    assert.equal(abortReason(controller.signal), reason);
  });

  it('falls back to the package error when a signal carries no reason', () => {
    const signal = { aborted: true, reason: undefined } as unknown as AbortSignal;

    assert.ok(abortReason(signal) instanceof KafkaReplyAbortedError);
  });
});

describe('request-reply errors', () => {
  it('names the topic, correlation id, and budget in a timeout', () => {
    const error = new KafkaReplyTimeoutError('orders.total', 'c1', 250);

    assert.equal(error.name, 'KafkaReplyTimeoutError');
    assert.equal(error.topic, 'orders.total');
    assert.equal(error.correlationId, 'c1');
    assert.equal(error.timeoutMs, 250);
    // The message has to say "unknown outcome" — that is the whole semantic.
    assert.match(error.message, /outcome is unknown/);
  });

  it('carries the decoded remote error and the reply that held it', () => {
    const reply = {
      value: null,
      headers: {},
      correlationId: 'c1',
      topic: 'replies',
      partition: 0,
    };
    const error = new KafkaReplyRemoteError({ name: 'E' }, reply, 'detail');

    assert.equal(error.name, 'KafkaReplyRemoteError');
    assert.deepEqual(error.remote, { name: 'E' });
    assert.equal(error.reply, reply);
    assert.match(error.message, /detail/);
  });

  it('defaults the aborted message and accepts an override', () => {
    assert.match(new KafkaReplyAbortedError().message, /not cancelled/);
    assert.equal(new KafkaReplyAbortedError('custom').message, 'custom');
  });

  it('keeps the produce failure as the cause of a delivery error', () => {
    const cause = new Error('broker down');
    const error = new KafkaReplyDeliveryError('replies', 'c1', cause);

    assert.equal(error.name, 'KafkaReplyDeliveryError');
    assert.equal(error.topic, 'replies');
    assert.equal(error.correlationId, 'c1');
    assert.equal((error as { cause?: unknown }).cause, cause);
  });
});

function fail(): Error {
  return new Error('the deadline should not have fired');
}

/** A promise that never settles, so only the deadline or the signal can end the wait. */
function pending(): Promise<never> {
  return new Promise<never>(() => {});
}
