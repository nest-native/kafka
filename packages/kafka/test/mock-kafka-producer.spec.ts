import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { KafkaProducerService } from '../kafka-producer.service';
import {
  createMockKafkaProducer,
  createMockTransaction,
} from '../testing/mock-kafka-producer';

describe('createMockKafkaProducer', () => {
  it('records the producer lifecycle through the producer service', async () => {
    const { producer, calls } = createMockKafkaProducer();
    const service = new KafkaProducerService(producer);

    await service.onModuleInit();
    assert.equal(calls.connect, 1);

    await service.onApplicationShutdown();
    assert.equal(calls.disconnect, 1);
  });

  it('records single sends and returns one metadata entry per message', async () => {
    const { producer, calls } = createMockKafkaProducer();

    const result = await producer.send({
      topic: 'orders',
      messages: [{ value: 'a' }, { value: 'b' }],
    });

    assert.equal(calls.send.length, 1);
    assert.deepEqual(calls.send[0].topic, 'orders');
    assert.deepEqual(result, [
      { topicName: 'orders', partition: 0, errorCode: 0, offset: '0' },
      { topicName: 'orders', partition: 1, errorCode: 0, offset: '1' },
    ]);
  });

  it('records batch sends and returns metadata across topics', async () => {
    const { producer, calls } = createMockKafkaProducer();

    const result = await producer.sendBatch({
      topicMessages: [
        { topic: 'orders', messages: [{ value: 'a' }] },
        { topic: 'audit', messages: [{ value: 'b' }, { value: 'c' }] },
      ],
    });

    assert.equal(calls.sendBatch.length, 1);
    assert.equal(result.length, 3);
    assert.deepEqual(
      result.map(entry => entry.topicName),
      ['orders', 'audit', 'audit'],
    );
  });

  it('treats a batch with no topicMessages as an empty batch', async () => {
    const { producer } = createMockKafkaProducer();

    const result = await producer.sendBatch({});

    assert.deepEqual(result, []);
  });

  it('records a transaction per call with its own send/commit bookkeeping', async () => {
    const { producer, calls, transactions } = createMockKafkaProducer();

    await producer.transaction().then(async tx => {
      await tx.send({ topic: 'orders', messages: [{ value: 'a' }] });
      await tx.sendBatch({
        topicMessages: [{ topic: 'audit', messages: [{ value: 'b' }] }],
      });
      await tx.sendOffsets({ consumer: {} as never, topics: [] });
      await tx.commit();
    });

    assert.equal(calls.transaction, 1);
    assert.equal(transactions.length, 1);

    const [recorded] = transactions;
    assert.equal(recorded.calls.send.length, 1);
    assert.equal(recorded.calls.sendBatch.length, 1);
    assert.equal(recorded.calls.sendOffsets.length, 1);
    assert.equal(recorded.calls.commit, 1);
    assert.equal(recorded.calls.abort, 0);
  });

  it('aborts the transaction through the producer service when work fails', async () => {
    const { producer, transactions } = createMockKafkaProducer();
    const service = new KafkaProducerService(producer);
    const failure = new Error('boom');

    await assert.rejects(
      service.transactional(() => {
        throw failure;
      }),
      failure,
    );

    assert.equal(transactions.length, 1);
    assert.equal(transactions[0].calls.commit, 0);
    assert.equal(transactions[0].calls.abort, 1);
  });

  it('resets every recorded call and transaction', async () => {
    const mock = createMockKafkaProducer();

    await mock.producer.connect();
    await mock.producer.send({ topic: 'orders', messages: [{ value: 'a' }] });
    await mock.producer.transaction();

    mock.reset();

    assert.deepEqual(mock.calls, {
      connect: 0,
      disconnect: 0,
      send: [],
      sendBatch: [],
      transaction: 0,
    });
    assert.deepEqual(mock.transactions, []);
  });
});

describe('createMockTransaction', () => {
  it('records sends, offsets, commit, and abort directly', async () => {
    const { transaction, calls } = createMockTransaction();

    const sendResult = await transaction.send({
      topic: 'orders',
      messages: [{ value: 'a' }],
    });
    const batchResult = await transaction.sendBatch({
      topicMessages: [{ topic: 'audit', messages: [{ value: 'b' }] }],
    });
    await transaction.sendOffsets({ consumer: {} as never, topics: [] });
    await transaction.commit();
    await transaction.abort();

    assert.deepEqual(sendResult, [
      { topicName: 'orders', partition: 0, errorCode: 0, offset: '0' },
    ]);
    assert.deepEqual(batchResult, [
      { topicName: 'audit', partition: 0, errorCode: 0, offset: '0' },
    ]);
    assert.equal(calls.send.length, 1);
    assert.equal(calls.sendBatch.length, 1);
    assert.equal(calls.sendOffsets.length, 1);
    assert.equal(calls.commit, 1);
    assert.equal(calls.abort, 1);
  });
});
