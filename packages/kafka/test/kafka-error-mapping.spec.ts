import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import {
  BadRequestException,
  HttpException,
  InternalServerErrorException,
} from '@nestjs/common';
import {
  applyKafkaErrorBehavior,
  defaultKafkaErrorMapper,
  KafkaErrorMapper,
} from '../kafka-error-mapping';
import { KafkaContext } from '../kafka-context';

const context = new KafkaContext('topic', 0, { value: null });

describe('defaultKafkaErrorMapper', () => {
  it('commits a 4xx HttpException (non-retryable client error)', () => {
    assert.equal(
      defaultKafkaErrorMapper(new BadRequestException('bad'), context),
      'commit',
    );
  });

  it('retries a 5xx HttpException (transient server error)', () => {
    assert.equal(
      defaultKafkaErrorMapper(new InternalServerErrorException('boom'), context),
      'retry',
    );
  });

  it('retries a sub-4xx HttpException — only 4xx commits, the rest are transient', () => {
    // HttpException can carry any status; the mapper commits ONLY the 4xx band,
    // so a sub-400 status (e.g. a stray 3xx) is transient and retried. This
    // exercises the lower `status >= 400` bound of the commit window.
    assert.equal(
      defaultKafkaErrorMapper(new HttpException('below 4xx', 399), context),
      'retry',
    );
  });

  it('retries a plain Error (unknown → transient)', () => {
    assert.equal(
      defaultKafkaErrorMapper(new Error('downstream timeout'), context),
      'retry',
    );
  });
});

describe('applyKafkaErrorBehavior', () => {
  it('resolves when the mapper says commit', async () => {
    const commit: KafkaErrorMapper = () => 'commit';
    await assert.doesNotReject(applyKafkaErrorBehavior(new Error('x'), context, commit));
  });

  it('rejects with the original error when the mapper says retry', async () => {
    const retry: KafkaErrorMapper = () => 'retry';
    const failure = new Error('redeliver me');
    await assert.rejects(
      applyKafkaErrorBehavior(failure, context, retry),
      (error: unknown) => error === failure,
    );
  });

  it('awaits an async mapper before deciding', async () => {
    const failure = new Error('redeliver me');
    const later = <T>(value: T): Promise<T> =>
      new Promise(resolve => setTimeout(() => resolve(value), 5));

    await assert.doesNotReject(
      applyKafkaErrorBehavior(failure, context, () => later('commit' as const)),
    );
    // A promise never equals 'retry'; comparing it unawaited committed this.
    await assert.rejects(
      applyKafkaErrorBehavior(failure, context, () => later('retry' as const)),
      (error: unknown) => error === failure,
    );
  });

  it('rejects with the mapper\'s own error when the mapper fails', async () => {
    const unavailable = new Error('dead-letter topic unavailable');
    const failing: KafkaErrorMapper[] = [
      () => {
        throw unavailable;
      },
      async () => {
        throw unavailable;
      },
    ];
    for (const mapper of failing) {
      await assert.rejects(
        applyKafkaErrorBehavior(new Error('handler failed'), context, mapper),
        (error: unknown) => error === unavailable,
      );
    }
  });

  it('passes the error and context to a custom mapper', async () => {
    const seen: { error: unknown; topic: string }[] = [];
    const mapper: KafkaErrorMapper = (error, ctx) => {
      seen.push({ error, topic: ctx.getTopic() });
      return 'commit';
    };
    const failure = new Error('inspect me');

    await applyKafkaErrorBehavior(failure, context, mapper);

    assert.deepEqual(seen, [{ error: failure, topic: 'topic' }]);
  });
});
