import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import {
  BadRequestException,
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

  it('retries a plain Error (unknown → transient)', () => {
    assert.equal(
      defaultKafkaErrorMapper(new Error('downstream timeout'), context),
      'retry',
    );
  });
});

describe('applyKafkaErrorBehavior', () => {
  it('returns without throwing when the mapper says commit', () => {
    const commit: KafkaErrorMapper = () => 'commit';
    assert.doesNotThrow(() =>
      applyKafkaErrorBehavior(new Error('x'), context, commit),
    );
  });

  it('rethrows the original error when the mapper says retry', () => {
    const retry: KafkaErrorMapper = () => 'retry';
    const failure = new Error('redeliver me');
    assert.throws(
      () => applyKafkaErrorBehavior(failure, context, retry),
      (error: unknown) => error === failure,
    );
  });

  it('passes the error and context to a custom mapper', () => {
    const seen: { error: unknown; topic: string }[] = [];
    const mapper: KafkaErrorMapper = (error, ctx) => {
      seen.push({ error, topic: ctx.getTopic() });
      return 'commit';
    };
    const failure = new Error('inspect me');

    applyKafkaErrorBehavior(failure, context, mapper);

    assert.deepEqual(seen, [{ error: failure, topic: 'topic' }]);
  });
});
