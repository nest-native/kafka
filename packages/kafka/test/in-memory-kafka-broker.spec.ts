import assert from 'node:assert/strict';
import { beforeEach, describe, it } from 'node:test';
import {
  KafkaEachBatchPayload,
  KafkaEachMessagePayload,
} from '../driver';
import { InMemoryKafkaBroker } from '../testing/in-memory-kafka-broker';

describe('InMemoryKafkaBroker', () => {
  let broker: InMemoryKafkaBroker;

  beforeEach(() => {
    broker = new InMemoryKafkaBroker();
  });

  it('loops a produced message to a subscribed per-message consumer', async () => {
    const driver = broker.createDriver();
    const received: KafkaEachMessagePayload[] = [];

    const consumer = driver.createConsumer({ groupId: 'g' });
    await consumer.subscribe({ topics: ['orders'] });
    await consumer.run({ eachMessage: async payload => void received.push(payload) });

    const producer = driver.createProducer();
    await producer.send({ topic: 'orders', messages: [{ value: 'a' }] });

    assert.equal(received.length, 1);
    assert.equal(received[0].topic, 'orders');
    assert.equal(received[0].partition, 0);
    assert.equal(received[0].message.value, 'a');
    assert.equal(received[0].message.key, null);
    assert.equal(received[0].message.offset, '0');
  });

  it('does not deliver to a consumer subscribed to another topic', async () => {
    const driver = broker.createDriver();
    let calls = 0;

    const consumer = driver.createConsumer();
    await consumer.subscribe({ topics: ['other'] });
    await consumer.run({ eachMessage: async () => void (calls += 1) });

    await driver.createProducer().send({ topic: 'orders', messages: [{ value: 'a' }] });

    assert.equal(calls, 0);
  });

  it('preserves an explicit message key when delivering', async () => {
    const driver = broker.createDriver();
    const received: KafkaEachMessagePayload[] = [];

    const consumer = driver.createConsumer();
    await consumer.subscribe({ topics: ['orders'] });
    await consumer.run({ eachMessage: async payload => void received.push(payload) });

    await driver
      .createProducer()
      .send({ topic: 'orders', messages: [{ key: 'k', value: 'a' }] });

    assert.equal(received[0].message.key, 'k');
  });

  it('delivers a sendBatch across topics to the right consumers', async () => {
    const driver = broker.createDriver();
    const ordersSeen: string[] = [];
    const auditSeen: string[] = [];

    const ordersConsumer = driver.createConsumer();
    await ordersConsumer.subscribe({ topics: ['orders'] });
    await ordersConsumer.run({
      eachMessage: async payload =>
        void ordersSeen.push(String(payload.message.value)),
    });

    const auditConsumer = driver.createConsumer();
    await auditConsumer.subscribe({ topics: ['audit'] });
    await auditConsumer.run({
      eachMessage: async payload =>
        void auditSeen.push(String(payload.message.value)),
    });

    const result = await driver.createProducer().sendBatch({
      topicMessages: [
        { topic: 'orders', messages: [{ value: 'o1' }] },
        { topic: 'audit', messages: [{ value: 'a1' }, { value: 'a2' }] },
      ],
    });

    assert.deepEqual(ordersSeen, ['o1']);
    assert.deepEqual(auditSeen, ['a1', 'a2']);
    assert.equal(result.length, 3);
  });

  it('treats a sendBatch with no topicMessages as a no-op', async () => {
    const driver = broker.createDriver();
    const result = await driver.createProducer().sendBatch({});
    assert.deepEqual(result, []);
    assert.deepEqual(broker.getSent(), []);
  });

  it('delivers to a batch consumer grouped by partition', async () => {
    const driver = broker.createDriver();
    const batches: KafkaEachBatchPayload['batch'][] = [];

    const consumer = driver.createConsumer();
    await consumer.subscribe({ topics: ['metrics'] });
    await consumer.run({
      eachBatch: async payload => {
        batches.push(payload.batch);
        payload.resolveOffset('0');
      },
    });

    await driver.createProducer().send({
      topic: 'metrics',
      messages: [
        { partition: 0, value: 'p0a' },
        { partition: 1, value: 'p1a' },
        { partition: 0, value: 'p0b' },
      ],
    });

    assert.equal(batches.length, 2);
    const partition0 = batches.find(batch => batch.partition === 0);
    const partition1 = batches.find(batch => batch.partition === 1);
    assert.deepEqual(
      partition0?.messages.map(message => message.value),
      ['p0a', 'p0b'],
    );
    assert.deepEqual(
      partition1?.messages.map(message => message.value),
      ['p1a'],
    );
  });

  it('defaults a message with no partition to partition 0 in batch mode', async () => {
    const driver = broker.createDriver();
    const partitions: number[] = [];

    const consumer = driver.createConsumer();
    await consumer.subscribe({ topics: ['metrics'] });
    await consumer.run({
      eachBatch: async payload => void partitions.push(payload.batch.partition),
    });

    await driver
      .createProducer()
      .send({ topic: 'metrics', messages: [{ value: 'a' }] });

    assert.deepEqual(partitions, [0]);
  });

  it('isolates a throwing per-message handler so the producer still resolves', async () => {
    const driver = broker.createDriver();

    const consumer = driver.createConsumer();
    await consumer.subscribe({ topics: ['orders'] });
    await consumer.run({
      eachMessage: async () => {
        throw new Error('handler exploded');
      },
    });

    await assert.doesNotReject(
      driver.createProducer().send({ topic: 'orders', messages: [{ value: 'a' }] }),
    );
  });

  it('isolates a throwing batch handler so the producer still resolves', async () => {
    const driver = broker.createDriver();

    const consumer = driver.createConsumer();
    await consumer.subscribe({ topics: ['metrics'] });
    await consumer.run({
      eachBatch: async () => {
        throw new Error('batch exploded');
      },
    });

    await assert.doesNotReject(
      driver.createProducer().send({ topic: 'metrics', messages: [{ value: 'a' }] }),
    );
  });

  it('stops delivering after the consumer disconnects', async () => {
    const driver = broker.createDriver();
    let calls = 0;

    const consumer = driver.createConsumer();
    await consumer.connect();
    await consumer.subscribe({ topics: ['orders'] });
    await consumer.run({ eachMessage: async () => void (calls += 1) });

    const producer = driver.createProducer();
    await producer.connect();
    await producer.send({ topic: 'orders', messages: [{ value: 'a' }] });
    await consumer.disconnect();
    await producer.send({ topic: 'orders', messages: [{ value: 'b' }] });
    await producer.disconnect();

    assert.equal(calls, 1);
  });

  it('emits an external message to a subscribed consumer and records it', async () => {
    const driver = broker.createDriver();
    const received: string[] = [];

    const consumer = driver.createConsumer();
    await consumer.subscribe({ topics: ['orders'] });
    await consumer.run({
      eachMessage: async payload => void received.push(String(payload.message.value)),
    });

    await broker.emit('orders', { value: 'injected' });

    assert.deepEqual(received, ['injected']);
    assert.deepEqual(broker.getSentTo('orders'), [{ value: 'injected' }]);
  });

  it('records every delivered message and filters by topic', async () => {
    const producer = broker.createDriver().createProducer();
    await producer.send({ topic: 'orders', messages: [{ value: 'o1' }] });
    await producer.sendBatch({
      topicMessages: [{ topic: 'audit', messages: [{ value: 'a1' }] }],
    });

    assert.deepEqual(
      broker.getSent().map(record => record.topic),
      ['orders', 'audit'],
    );
    assert.deepEqual(broker.getSentTo('orders'), [{ value: 'o1' }]);
    assert.deepEqual(broker.getSentTo('audit'), [{ value: 'a1' }]);
    assert.deepEqual(broker.getSentTo('missing'), []);
  });

  it('resets recorded messages while keeping consumers subscribed', async () => {
    const driver = broker.createDriver();
    const received: string[] = [];

    const consumer = driver.createConsumer();
    await consumer.subscribe({ topics: ['orders'] });
    await consumer.run({
      eachMessage: async payload => void received.push(String(payload.message.value)),
    });

    const producer = driver.createProducer();
    await producer.send({ topic: 'orders', messages: [{ value: 'a' }] });
    broker.reset();
    assert.deepEqual(broker.getSent(), []);

    await producer.send({ topic: 'orders', messages: [{ value: 'b' }] });
    assert.deepEqual(broker.getSentTo('orders'), [{ value: 'b' }]);
    assert.deepEqual(received, ['a', 'b']);
  });

  it('commits a transaction so its buffered writes are delivered', async () => {
    const driver = broker.createDriver();
    const received: string[] = [];

    const consumer = driver.createConsumer();
    await consumer.subscribe({ topics: ['orders'] });
    await consumer.run({
      eachMessage: async payload => void received.push(String(payload.message.value)),
    });

    const producer = driver.createProducer();
    const transaction = await producer.transaction();
    const sendResult = await transaction.send({
      topic: 'orders',
      messages: [{ value: 'a' }],
    });
    const batchResult = await transaction.sendBatch({
      topicMessages: [{ topic: 'orders', messages: [{ value: 'b' }] }],
    });
    await transaction.sendOffsets({ consumer: {} as never, topics: [] });

    // Nothing is delivered until commit.
    assert.deepEqual(received, []);
    assert.deepEqual(sendResult, [
      { topicName: 'orders', partition: 0, errorCode: 0, offset: '0' },
    ]);
    assert.deepEqual(batchResult, []);

    await transaction.commit();
    assert.deepEqual(received, ['a', 'b']);
  });

  it('aborts a transaction so its buffered writes are discarded', async () => {
    const driver = broker.createDriver();
    const received: string[] = [];

    const consumer = driver.createConsumer();
    await consumer.subscribe({ topics: ['orders'] });
    await consumer.run({
      eachMessage: async payload => void received.push(String(payload.message.value)),
    });

    const transaction = await driver.createProducer().transaction();
    await transaction.send({ topic: 'orders', messages: [{ value: 'a' }] });
    await transaction.abort();

    assert.deepEqual(received, []);
    assert.deepEqual(broker.getSent(), []);
  });

  describe('idle', () => {
    it('resolves when nothing is in flight', async () => {
      await broker.idle();
      assert.deepEqual(broker.getSent(), []);
    });

    it('awaits an async handler whose delivery the caller never awaited', async () => {
      const driver = broker.createDriver();
      const received: string[] = [];

      const consumer = driver.createConsumer();
      await consumer.subscribe({ topics: ['orders'] });
      await consumer.run({
        eachMessage: async payload => {
          await new Promise(resolve => setTimeout(resolve, 10));
          received.push(String(payload.message.value));
        },
      });

      // Fire-and-forget: the send is deliberately not awaited, exactly like a
      // service that publishes without blocking its caller.
      void driver
        .createProducer()
        .send({ topic: 'orders', messages: [{ value: 'a' }] });
      assert.deepEqual(received, [], 'the handler has not finished yet');

      await broker.idle();
      assert.deepEqual(received, ['a']);
    });

    it('follows a cascade where a handler triggers a further dispatch', async () => {
      const driver = broker.createDriver();
      const producer = driver.createProducer();
      const audited: string[] = [];

      // First hop: the orders handler produces an audit record it never awaits
      // (the DLQ/audit pattern). Second hop: the audit handler is slow.
      const ordersConsumer = driver.createConsumer();
      await ordersConsumer.subscribe({ topics: ['orders'] });
      await ordersConsumer.run({
        eachMessage: async payload => {
          await new Promise(resolve => setTimeout(resolve, 5));
          void producer.send({
            topic: 'audit',
            messages: [{ value: `audit:${String(payload.message.value)}` }],
          });
        },
      });

      const auditConsumer = driver.createConsumer();
      await auditConsumer.subscribe({ topics: ['audit'] });
      await auditConsumer.run({
        eachMessage: async payload => {
          await new Promise(resolve => setTimeout(resolve, 5));
          audited.push(String(payload.message.value));
        },
      });

      // Nothing is awaited: the audit dispatch enters the broker only while
      // idle() is already waiting on the first hop, so idle must loop — one
      // settle pass over the orders delivery cannot see the work it spawned.
      void producer.send({ topic: 'orders', messages: [{ value: 'o1' }] });
      await broker.idle();

      assert.deepEqual(audited, ['audit:o1']);
      assert.deepEqual(broker.getSentTo('audit'), [{ value: 'audit:o1' }]);
    });

    it('resolves even when the in-flight handler throws', async () => {
      const driver = broker.createDriver();

      const consumer = driver.createConsumer();
      await consumer.subscribe({ topics: ['orders'] });
      await consumer.run({
        eachMessage: async () => {
          await new Promise(resolve => setTimeout(resolve, 5));
          throw new Error('handler exploded');
        },
      });

      void driver
        .createProducer()
        .send({ topic: 'orders', messages: [{ value: 'a' }] });

      await assert.doesNotReject(broker.idle());
    });
  });
});
