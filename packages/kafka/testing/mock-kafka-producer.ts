import {
  KafkaDriverProducer,
  KafkaRecordMetadata,
  KafkaSendBatch,
  KafkaSendRecord,
  KafkaTransaction,
} from '../driver';

/**
 * A recording {@link KafkaDriverProducer} for unit tests, plus the calls it
 * captured. Returned by {@link createMockKafkaProducer}.
 *
 * @publicApi
 */
export interface MockKafkaProducer {
  /**
   * The producer to inject — pass it to `new KafkaProducerService(producer)` or
   * register it under the `KAFKA_PRODUCER` token — wherever the code under test
   * expects a {@link KafkaDriverProducer}.
   */
  producer: KafkaDriverProducer;

  /**
   * Every call the producer received, in order, so a test can assert exactly
   * what was published and how the producer's lifecycle was driven.
   */
  calls: MockKafkaProducerCalls;

  /**
   * The transactions opened through {@link KafkaDriverProducer.transaction}, each
   * with its own recorded calls. A new entry is pushed every time a transaction
   * is started, so a test can assert commit/abort per transaction.
   */
  transactions: MockKafkaTransaction[];

  /**
   * Forget every recorded call and transaction, for reuse between test phases.
   */
  reset(): void;
}

/**
 * The lifecycle and publish calls a {@link MockKafkaProducer} records.
 *
 * @publicApi
 */
export interface MockKafkaProducerCalls {
  connect: number;
  disconnect: number;
  send: KafkaSendRecord[];
  sendBatch: KafkaSendBatch[];
  transaction: number;
}

/**
 * A recorded transaction returned by a {@link MockKafkaProducer}: the
 * transaction handle itself plus the calls made on it.
 *
 * @publicApi
 */
export interface MockKafkaTransaction {
  transaction: KafkaTransaction;
  calls: MockKafkaTransactionCalls;
}

/**
 * The calls a mock {@link KafkaTransaction} records.
 *
 * @publicApi
 */
export interface MockKafkaTransactionCalls {
  send: KafkaSendRecord[];
  sendBatch: KafkaSendBatch[];
  sendOffsets: unknown[];
  commit: number;
  abort: number;
}

/**
 * Create a recording mock producer for unit tests.
 *
 * Use it to unit-test a service that injects the producer (via
 * `KafkaProducerService` or `@InjectKafkaProducer()`) without a broker or a Nest
 * module: assert against `calls`/`transactions`, no real Kafka required. For an
 * integration-style test that drives the consumer pipeline as well, prefer
 * {@link KafkaTestModule} with its {@link InMemoryKafkaBroker}.
 *
 * @example
 * ```ts
 * const { producer, calls } = createMockKafkaProducer();
 * const service = new KafkaProducerService(producer);
 * await service.send({ topic: 'orders', messages: [{ value: 'hi' }] });
 * assert.equal(calls.send.length, 1);
 * ```
 *
 * @publicApi
 */
export function createMockKafkaProducer(): MockKafkaProducer {
  const calls: MockKafkaProducerCalls = {
    connect: 0,
    disconnect: 0,
    send: [],
    sendBatch: [],
    transaction: 0,
  };
  const transactions: MockKafkaTransaction[] = [];

  const producer: KafkaDriverProducer = {
    async connect() {
      calls.connect += 1;
    },
    async disconnect() {
      calls.disconnect += 1;
    },
    async send(record) {
      calls.send.push(record);
      return record.messages.map((_, index) => metadata(record.topic, index));
    },
    async sendBatch(batch) {
      calls.sendBatch.push(batch);
      return collectBatchMetadata(batch);
    },
    async transaction() {
      calls.transaction += 1;
      const recorded = createMockTransaction();
      transactions.push(recorded);
      return recorded.transaction;
    },
  };

  return {
    producer,
    calls,
    transactions,
    reset() {
      calls.connect = 0;
      calls.disconnect = 0;
      calls.send.length = 0;
      calls.sendBatch.length = 0;
      calls.transaction = 0;
      transactions.length = 0;
    },
  };
}

/**
 * Create a recording mock transaction, used internally by
 * {@link createMockKafkaProducer} and exported for tests that drive a
 * transaction handle directly.
 *
 * @publicApi
 */
export function createMockTransaction(): MockKafkaTransaction {
  const calls: MockKafkaTransactionCalls = {
    send: [],
    sendBatch: [],
    sendOffsets: [],
    commit: 0,
    abort: 0,
  };

  const transaction: KafkaTransaction = {
    async send(record) {
      calls.send.push(record);
      return record.messages.map((_, index) => metadata(record.topic, index));
    },
    async sendBatch(batch) {
      calls.sendBatch.push(batch);
      return collectBatchMetadata(batch);
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
  };

  return { transaction, calls };
}

function collectBatchMetadata(batch: KafkaSendBatch): KafkaRecordMetadata[] {
  const results: KafkaRecordMetadata[] = [];
  for (const topicMessages of batch.topicMessages ?? []) {
    topicMessages.messages.forEach((_, index) =>
      results.push(metadata(topicMessages.topic, index)),
    );
  }
  return results;
}

function metadata(topic: string, partition: number): KafkaRecordMetadata {
  return {
    topicName: topic,
    partition,
    errorCode: 0,
    offset: String(partition),
  };
}
