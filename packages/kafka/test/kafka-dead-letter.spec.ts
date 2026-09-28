import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { BadRequestException } from '@nestjs/common';
import { KafkaMessageHeaders } from '../driver';
import { KafkaBatchContext, KafkaContext } from '../kafka-context';
import {
  KAFKA_DEAD_LETTER_HEADERS as H,
  readDeadLetterHeaders,
  toDeadLetterMessage,
  toDeadLetterMessages,
} from '../kafka-dead-letter';

const poison = new KafkaContext('orders', 3, {
  key: 'order-7',
  value: '{"id":7',
  offset: '41',
  timestamp: '1790000000123',
  headers: { 'trace-id': 'abc', [H.originalTopic]: 'an-earlier-hop' },
});

function buffer(headers: KafkaMessageHeaders, name: string): Buffer {
  const value = headers[name];
  assert.ok(Buffer.isBuffer(value), `${name} is binary`);
  return value;
}

describe('toDeadLetterMessage', () => {
  it('keeps the original record and adds the Spring dead-letter headers', () => {
    const error = new BadRequestException('unparseable order');
    const record = toDeadLetterMessage(poison, error, { consumerGroup: 'billing' });
    const headers = record.headers ?? {};

    assert.equal(record.key, 'order-7');
    assert.equal(record.value, '{"id":7');
    assert.equal(headers['trace-id'], 'abc', 'original headers survive');
    // An earlier hop's header is replaced by this one's.
    assert.equal(headers[H.originalTopic], 'orders');
    assert.equal(headers[H.originalConsumerGroup], 'billing');
    assert.equal(headers[H.exceptionFqcn], 'BadRequestException');
    assert.equal(headers[H.exceptionMessage], 'unparseable order');
    assert.match(String(headers[H.exceptionStacktrace]), /BadRequestException: unparseable order/);

    // Spring's binary encodings: int32 partition, int64 offset and timestamp.
    assert.deepEqual(buffer(headers, H.originalPartition), Buffer.from([0, 0, 0, 3]));
    assert.equal(buffer(headers, H.originalOffset).readBigInt64BE(), 41n);
    assert.equal(buffer(headers, H.originalTimestamp).readBigInt64BE(), 1790000000123n);
  });

  it('leaves out what it does not know, and the stack trace on request', () => {
    const bare = new KafkaContext('orders', 0, { value: null });
    const record = toDeadLetterMessage(bare, new Error('boom'), {
      includeStackTrace: false,
    });
    const headers = record.headers ?? {};

    assert.equal('key' in record, false, 'no key invented');
    assert.equal(record.value, null);
    for (const absent of [
      H.originalOffset,
      H.originalTimestamp,
      H.originalConsumerGroup,
      H.exceptionStacktrace,
    ]) {
      assert.equal(headers[absent], undefined, `${absent} is absent`);
    }
    assert.deepEqual(buffer(headers, H.originalPartition), Buffer.from([0, 0, 0, 0]));
  });

  it('keeps a null key, and skips an offset that is not an integer', () => {
    const odd = new KafkaContext('orders', 0, { key: null, value: 'x', offset: 'n/a' });
    const record = toDeadLetterMessage(odd, new Error('boom'));

    assert.equal(record.key, null);
    assert.equal(record.headers?.[H.originalOffset], undefined);
  });

  it('describes a thrown value that is not an Error', () => {
    const stackless = new TypeError('no stack');
    stackless.stack = undefined;
    for (const [thrown, fqcn, message] of [
      ['a string', 'string', 'a string'],
      [null, 'null', 'null'],
      [{ code: 7 }, 'object', '[object Object]'],
      [stackless, 'TypeError', 'no stack'],
    ] as const) {
      const headers = toDeadLetterMessage(poison, thrown).headers ?? {};
      assert.equal(headers[H.exceptionFqcn], fqcn);
      assert.equal(headers[H.exceptionMessage], message);
      assert.equal(headers[H.exceptionStacktrace], undefined);
    }
  });
});

describe('toDeadLetterMessages', () => {
  it('dead-letters every message of a failed batch, in order', () => {
    const batch = new KafkaBatchContext({
      topic: 'metrics',
      partition: 2,
      messages: [
        { value: 'a', offset: '10' },
        { value: 'b', offset: '11' },
      ],
    });
    const records = toDeadLetterMessages(batch, new Error('warehouse down'));

    assert.deepEqual(
      records.map(record => [
        record.value,
        readDeadLetterHeaders(record.headers)?.originalOffset,
        readDeadLetterHeaders(record.headers)?.originalPartition,
      ]),
      [
        ['a', '10', 2],
        ['b', '11', 2],
      ],
    );
  });
});

describe('readDeadLetterHeaders', () => {
  it('decodes what toDeadLetterMessage wrote', () => {
    const error = new BadRequestException('unparseable order');
    const headers = toDeadLetterMessage(poison, error, { consumerGroup: 'billing' }).headers;

    assert.deepEqual(
      { ...readDeadLetterHeaders(headers), exceptionStacktrace: undefined },
      {
        originalTopic: 'orders',
        originalPartition: 3,
        originalOffset: '41',
        originalTimestamp: '1790000000123',
        originalConsumerGroup: 'billing',
        exceptionFqcn: 'BadRequestException',
        exceptionMessage: 'unparseable order',
        exceptionStacktrace: undefined,
      },
    );
  });

  it('reads the headers as a consumer delivers them: buffers, the last value wins', () => {
    const partition = Buffer.alloc(4);
    partition.writeInt32BE(5);
    const headers: KafkaMessageHeaders = {
      [H.originalTopic]: [Buffer.from('older'), Buffer.from('orders')],
      [H.originalPartition]: partition,
      [H.exceptionMessage]: Buffer.from('boom'),
    };

    const info = readDeadLetterHeaders(headers);
    assert.equal(info?.originalTopic, 'orders');
    assert.equal(info?.originalPartition, 5);
    assert.equal(info?.exceptionMessage, 'boom');
    assert.equal(info?.originalOffset, undefined);
  });

  it('accepts decimal strings from other producers, even eight digits long', () => {
    const info = readDeadLetterHeaders({
      [H.originalTopic]: 'orders',
      [H.originalPartition]: '1234',
      [H.originalOffset]: '12345678',
      [H.originalTimestamp]: Buffer.from('1790000000123'),
    });

    // Four and eight digits are exactly the binary widths; text still wins.
    assert.equal(info?.originalPartition, 1234);
    assert.equal(info?.originalOffset, '12345678');
    assert.equal(info?.originalTimestamp, '1790000000123');
  });

  it('ignores a numeric header it cannot read', () => {
    const info = readDeadLetterHeaders({
      [H.originalTopic]: 'orders',
      [H.originalPartition]: Buffer.from([0, 1]),
      [H.originalOffset]: Buffer.from([0, 0, 1]),
    });

    assert.equal(info?.originalPartition, undefined);
    assert.equal(info?.originalOffset, undefined);
  });

  it('leaves every field it does not find undefined', () => {
    assert.deepEqual(readDeadLetterHeaders({ [H.originalTopic]: 'orders' }), {
      originalTopic: 'orders',
      originalPartition: undefined,
      originalOffset: undefined,
      originalTimestamp: undefined,
      originalConsumerGroup: undefined,
      exceptionFqcn: undefined,
      exceptionMessage: undefined,
      exceptionStacktrace: undefined,
    });
  });

  it('is undefined for a record that is not a dead letter', () => {
    assert.equal(readDeadLetterHeaders(undefined), undefined);
    assert.equal(readDeadLetterHeaders({ 'trace-id': 'abc' }), undefined);
    assert.equal(readDeadLetterHeaders({ [H.originalTopic]: [] }), undefined);
  });
});
