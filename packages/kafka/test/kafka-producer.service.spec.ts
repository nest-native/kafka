import assert from 'node:assert/strict';
import { beforeEach, describe, it } from 'node:test';
import {
  KafkaDriverProducer,
  KafkaRecordMetadata,
  KafkaSendBatch,
  KafkaSendRecord,
  KafkaTransaction,
} from '../driver';
import { KafkaProducerService } from '../kafka-producer.service';

interface ProducerCalls {
  connect: number;
  disconnect: number;
  send: KafkaSendRecord[];
  sendBatch: KafkaSendBatch[];
  transaction: number;
}

interface TransactionCalls {
  send: KafkaSendRecord[];
  sendBatch: KafkaSendBatch[];
  sendOffsets: unknown[];
  commit: number;
  abort: number;
}

function metadata(topic: string): KafkaRecordMetadata[] {
  return [{ topicName: topic, partition: 0, errorCode: 0 }];
}

function createTransaction(overrides: Partial<KafkaTransaction> = {}): {
  transaction: KafkaTransaction;
  calls: TransactionCalls;
} {
  const calls: TransactionCalls = {
    send: [],
    sendBatch: [],
    sendOffsets: [],
    commit: 0,
    abort: 0,
  };

  const transaction: KafkaTransaction = {
    async send(record) {
      calls.send.push(record);
      return metadata(record.topic);
    },
    async sendBatch(batch) {
      calls.sendBatch.push(batch);
      return [];
    },
    async sendOffsets(offsets) {
      calls.sendOffsets.push(offsets);
    },
    async commit() {
      calls.commit += 1;
    },
    async abort() {
      calls.abort += 1;
    },
    ...overrides,
  };

  return { transaction, calls };
}

function createProducer(transaction?: KafkaTransaction): {
  producer: KafkaDriverProducer;
  calls: ProducerCalls;
} {
  const calls: ProducerCalls = {
    connect: 0,
    disconnect: 0,
    send: [],
    sendBatch: [],
    transaction: 0,
  };

  const producer: KafkaDriverProducer = {
    async connect() {
      calls.connect += 1;
    },
    async disconnect() {
      calls.disconnect += 1;
    },
    async send(record) {
      calls.send.push(record);
      return metadata(record.topic);
    },
    async sendBatch(batch) {
      calls.sendBatch.push(batch);
      return [];
    },
    async transaction() {
      calls.transaction += 1;
      return transaction ?? createTransaction().transaction;
    },
  };

  return { producer, calls };
}

describe('KafkaProducerService', () => {
  let producer: KafkaDriverProducer;
  let calls: ProducerCalls;
  let service: KafkaProducerService;

  beforeEach(() => {
    ({ producer, calls } = createProducer());
    service = new KafkaProducerService(producer);
  });

  it('connects exactly once on module init and reports the connection state', async () => {
    assert.equal(service.isConnected(), false);

    await service.onModuleInit();

    assert.equal(service.isConnected(), true);
    assert.equal(calls.connect, 1);
  });

  it('does not reconnect when already connected', async () => {
    await service.connect();
    await service.connect();

    assert.equal(calls.connect, 1);
  });

  it('disconnects on application shutdown only when connected', async () => {
    await service.onApplicationShutdown();
    assert.equal(calls.disconnect, 0);

    await service.connect();
    await service.onApplicationShutdown();

    assert.equal(calls.disconnect, 1);
    assert.equal(service.isConnected(), false);
  });

  it('is idempotent on a second disconnect', async () => {
    await service.connect();
    await service.disconnect();
    await service.disconnect();

    assert.equal(calls.disconnect, 1);
  });

  it('publishes a single record through send', async () => {
    const record: KafkaSendRecord = {
      topic: 'orders',
      messages: [{ value: 'payload' }],
    };

    const result = await service.send(record);

    assert.deepEqual(calls.send, [record]);
    assert.deepEqual(result, metadata('orders'));
  });

  it('publishes a batch through sendBatch', async () => {
    const batch: KafkaSendBatch = {
      topicMessages: [{ topic: 'orders', messages: [{ value: 'payload' }] }],
    };

    const result = await service.sendBatch(batch);

    assert.deepEqual(calls.sendBatch, [batch]);
    assert.deepEqual(result, []);
  });

  it('commits a transaction and returns the work result', async () => {
    const { transaction, calls: txCalls } = createTransaction();
    ({ producer, calls } = createProducer(transaction));
    service = new KafkaProducerService(producer);

    const result = await service.transactional(async tx => {
      await tx.send({ topic: 'orders', messages: [{ value: 'a' }] });
      return 'done';
    });

    assert.equal(result, 'done');
    assert.equal(calls.transaction, 1);
    assert.equal(txCalls.commit, 1);
    assert.equal(txCalls.abort, 0);
    assert.equal(txCalls.send.length, 1);
  });

  it('commits consumer offsets atomically inside a transaction', async () => {
    const { transaction, calls: txCalls } = createTransaction();
    ({ producer, calls } = createProducer(transaction));
    service = new KafkaProducerService(producer);

    const consumer = {} as never;
    const offsets = {
      consumer,
      topics: [
        { topic: 'orders', partitions: [{ partition: 0, offset: '11' }] },
      ],
    };

    await service.transactional(async tx => {
      await tx.send({ topic: 'audit', messages: [{ value: 'a' }] });
      await tx.sendOffsets(offsets);
    });

    assert.equal(txCalls.commit, 1);
    assert.equal(txCalls.abort, 0);
    assert.deepEqual(txCalls.sendOffsets, [offsets]);
  });

  it('aborts the transaction and rethrows when the work fails', async () => {
    const { transaction, calls: txCalls } = createTransaction();
    ({ producer, calls } = createProducer(transaction));
    service = new KafkaProducerService(producer);

    const failure = new Error('handler exploded');

    await assert.rejects(
      service.transactional(() => {
        throw failure;
      }),
      failure,
    );

    assert.equal(txCalls.commit, 0);
    assert.equal(txCalls.abort, 1);
  });

  it('preserves the original error and attaches the abort failure as its cause', async () => {
    const abortError = new Error('abort failed');
    const { transaction } = createTransaction({
      async abort() {
        throw abortError;
      },
    });
    ({ producer, calls } = createProducer(transaction));
    service = new KafkaProducerService(producer);

    const failure = new Error('handler exploded');

    await assert.rejects(
      service.transactional(() => {
        throw failure;
      }),
      (error: unknown) => {
        assert.equal(error, failure);
        assert.equal((error as { cause?: unknown }).cause, abortError);
        return true;
      },
    );
  });

  it('does not overwrite an existing cause when the abort also fails', async () => {
    const { transaction } = createTransaction({
      async abort() {
        throw new Error('abort failed');
      },
    });
    ({ producer, calls } = createProducer(transaction));
    service = new KafkaProducerService(producer);

    const existingCause = new Error('root');
    const failure = new Error('handler exploded');
    (failure as { cause?: unknown }).cause = existingCause;

    await assert.rejects(
      service.transactional(() => {
        throw failure;
      }),
      (error: unknown) => {
        assert.equal((error as { cause?: unknown }).cause, existingCause);
        return true;
      },
    );
  });

  it('leaves a non-Error thrown value untouched when the abort fails', async () => {
    const { transaction } = createTransaction({
      async abort() {
        throw new Error('abort failed');
      },
    });
    ({ producer, calls } = createProducer(transaction));
    service = new KafkaProducerService(producer);

    await assert.rejects(
      service.transactional(() => {
        throw 'plain string failure';
      }),
      (error: unknown) => {
        assert.equal(error, 'plain string failure');
        return true;
      },
    );
  });
});
