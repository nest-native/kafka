import assert from 'node:assert/strict';
import { afterEach, beforeEach, describe, it, mock } from 'node:test';
import { Logger } from '@nestjs/common';
import { KafkaTopicPartitions } from '../driver';
import {
  DEFAULT_KAFKA_RETRY_BACKOFF,
  KafkaPartitionPauser,
  KafkaRetryBackoff,
  resolveRetryBackoff,
} from '../kafka-retry-backoff';

/** Silence the backoff's warnings during the tests. */
Logger.overrideLogger(false);

interface RecordingPauser extends KafkaPartitionPauser {
  calls: string[];
}

function recordingPauser(): RecordingPauser {
  const calls: string[] = [];
  const describe = (topics: KafkaTopicPartitions[]): string =>
    topics.map(({ topic, partitions }) => `${topic}[${partitions}]`).join(',');
  return {
    calls,
    pause: topics => calls.push(`pause ${describe(topics)}`),
    resume: topics => calls.push(`resume ${describe(topics)}`),
  };
}

const OPTIONS = { initialDelayMs: 100, maxDelayMs: 350, multiplier: 2 };

describe('resolveRetryBackoff', () => {
  it('turns the backoff on with the defaults when the option is absent', () => {
    assert.deepEqual(resolveRetryBackoff(undefined), DEFAULT_KAFKA_RETRY_BACKOFF);
    assert.deepEqual(DEFAULT_KAFKA_RETRY_BACKOFF, {
      initialDelayMs: 1_000,
      maxDelayMs: 30_000,
      multiplier: 2,
    });
  });

  it('turns the backoff off for false', () => {
    assert.equal(resolveRetryBackoff(false), undefined);
  });

  it('fills the gaps of a partial option from the defaults', () => {
    assert.deepEqual(resolveRetryBackoff({ initialDelayMs: 250 }), {
      initialDelayMs: 250,
      maxDelayMs: 30_000,
      multiplier: 2,
    });
  });

  it('accepts a fixed delay and an immediate one', () => {
    assert.deepEqual(
      resolveRetryBackoff({ initialDelayMs: 0, maxDelayMs: 0, multiplier: 1 }),
      { initialDelayMs: 0, maxDelayMs: 0, multiplier: 1 },
    );
  });

  for (const [label, options] of [
    ['a negative initial delay', { initialDelayMs: -1 }],
    ['a cap below the initial delay', { initialDelayMs: 5_000, maxDelayMs: 1_000 }],
    ['a multiplier below 1', { multiplier: 0.5 }],
    ['a non-number', { initialDelayMs: Number.NaN }],
  ] as const) {
    it(`rejects ${label}`, () => {
      assert.throws(() => resolveRetryBackoff(options), /Invalid retryBackoff/);
    });
  }
});

describe('KafkaRetryBackoff', () => {
  beforeEach(() => {
    mock.timers.enable({ apis: ['setTimeout'] });
  });

  afterEach(() => {
    mock.timers.reset();
  });

  it('pauses only the failing partition, then resumes it after the delay', () => {
    const pauser = recordingPauser();
    const backoff = new KafkaRetryBackoff(OPTIONS, pauser);

    backoff.failed('orders', 3, '41');
    assert.deepEqual(pauser.calls, ['pause orders[3]']);

    mock.timers.tick(99);
    assert.deepEqual(pauser.calls, ['pause orders[3]'], 'still backing off');
    mock.timers.tick(1);
    assert.deepEqual(pauser.calls, ['pause orders[3]', 'resume orders[3]']);
  });

  it('grows the delay while the same record keeps failing, up to the cap', () => {
    const pauser = recordingPauser();
    const backoff = new KafkaRetryBackoff(OPTIONS, pauser);
    const resumedAfter: number[] = [];

    for (let attempt = 0; attempt < 4; attempt += 1) {
      pauser.calls.length = 0;
      backoff.failed('orders', 0, '7');
      let waited = 0;
      while (!pauser.calls.includes('resume orders[0]')) {
        mock.timers.tick(1);
        waited += 1;
      }
      resumedAfter.push(waited);
    }

    // 100, 200, 400 capped to 350, then the cap again.
    assert.deepEqual(resumedAfter, [100, 200, 350, 350]);
  });

  it('starts over when a different record fails or the partition succeeds', () => {
    const pauser = recordingPauser();
    const backoff = new KafkaRetryBackoff(OPTIONS, pauser);

    backoff.failed('orders', 0, '7');
    mock.timers.tick(100);
    backoff.failed('orders', 0, '7'); // second attempt: 200 ms
    mock.timers.tick(200);

    // A new record on the same partition is a new streak.
    pauser.calls.length = 0;
    backoff.failed('orders', 0, '8');
    mock.timers.tick(100);
    assert.deepEqual(pauser.calls, ['pause orders[0]', 'resume orders[0]']);

    // So is the same record after the partition handled something.
    backoff.succeeded('orders', 0);
    pauser.calls.length = 0;
    backoff.failed('orders', 0, '8');
    mock.timers.tick(100);
    assert.deepEqual(pauser.calls, ['pause orders[0]', 'resume orders[0]']);
  });

  it('keeps separate streaks per partition', () => {
    const pauser = recordingPauser();
    const backoff = new KafkaRetryBackoff(OPTIONS, pauser);

    backoff.failed('orders', 0, '1');
    mock.timers.tick(100);
    backoff.failed('orders', 0, '1'); // partition 0 now waits 200 ms
    backoff.failed('orders', 1, '1'); // partition 1 starts at 100 ms
    pauser.calls.length = 0;
    mock.timers.tick(100);
    assert.deepEqual(pauser.calls, ['resume orders[1]']);
    mock.timers.tick(100);
    assert.deepEqual(pauser.calls, ['resume orders[1]', 'resume orders[0]']);
  });

  it('cancels a pending resume when the partition succeeds', () => {
    const pauser = recordingPauser();
    const backoff = new KafkaRetryBackoff(OPTIONS, pauser);

    backoff.failed('orders', 0, '1');
    backoff.succeeded('orders', 0);
    backoff.succeeded('orders', 0); // nothing left to forget
    mock.timers.tick(1_000);
    assert.deepEqual(pauser.calls, ['pause orders[0]']);
  });

  it('cancels every pending resume on cancelAll', () => {
    const pauser = recordingPauser();
    const backoff = new KafkaRetryBackoff(OPTIONS, pauser);

    backoff.failed('orders', 0, '1');
    backoff.failed('refunds', 2, undefined);
    backoff.cancelAll();
    mock.timers.tick(1_000);
    assert.deepEqual(pauser.calls, ['pause orders[0]', 'pause refunds[2]']);
  });

  it('retries without a delay when the partition cannot be paused', () => {
    for (const failure of [new Error('not connected'), 'refused']) {
      const resumed: string[] = [];
      const backoff = new KafkaRetryBackoff(OPTIONS, {
        pause: () => {
          throw failure;
        },
        resume: () => resumed.push('resume'),
      });

      backoff.failed('orders', 0, '1');
      mock.timers.tick(1_000);
      // Nothing was paused, so there is nothing to resume; the client
      // redelivers as it does without a backoff.
      assert.deepEqual(resumed, []);
    }
  });

  it('shrugs off a resume that fails', () => {
    for (const failure of [new Error('Resume can only be called while connected.'), 'gone']) {
      let attempts = 0;
      const backoff = new KafkaRetryBackoff(OPTIONS, {
        pause: () => {},
        resume: () => {
          attempts += 1;
          throw failure;
        },
      });

      backoff.failed('orders', 0, '1');
      assert.doesNotThrow(() => mock.timers.tick(100));
      assert.equal(attempts, 1);
      // The streak survives, so the next failure still grows the delay.
      backoff.failed('orders', 0, '1');
      mock.timers.tick(199);
      assert.equal(attempts, 1);
      mock.timers.tick(1);
      assert.equal(attempts, 2);
    }
  });
});
