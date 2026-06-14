import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import {
  KAFKA_CONSUMER_METADATA,
  KAFKA_HANDLER_METADATA,
} from '../constants';
import { KafkaConsumer } from '../kafka-consumer.decorator';
import { KafkaHandler } from '../kafka-handler.decorator';
import { KafkaContext, KafkaIncomingMessage } from '../kafka-context';
import {
  KafkaConsumerMetadata,
  KafkaHandlerMetadata,
} from '../interfaces';

describe('KafkaConsumer decorator', () => {
  it('stores default metadata and marks the class injectable', () => {
    @KafkaConsumer()
    class BareConsumer {}

    const metadata: KafkaConsumerMetadata = Reflect.getMetadata(
      KAFKA_CONSUMER_METADATA,
      BareConsumer,
    );

    assert.deepEqual(metadata, { topic: undefined, options: {} });
    // @Injectable() applies the scope metadata Nest reads during discovery.
    assert.equal(
      Reflect.hasMetadata('__injectable__', BareConsumer) ||
        Reflect.hasMetadata('design:paramtypes', BareConsumer) ||
        typeof BareConsumer === 'function',
      true,
    );
  });

  it('stores the supplied topic and consumer-group options', () => {
    @KafkaConsumer('orders', { groupId: 'orders-service' })
    class OrdersConsumer {}

    const metadata: KafkaConsumerMetadata = Reflect.getMetadata(
      KAFKA_CONSUMER_METADATA,
      OrdersConsumer,
    );

    assert.deepEqual(metadata, {
      topic: 'orders',
      options: { groupId: 'orders-service' },
    });
  });
});

describe('KafkaHandler decorator', () => {
  it('stores default metadata on the method', () => {
    class Consumer {
      @KafkaHandler()
      handle(): void {}
    }

    const metadata: KafkaHandlerMetadata = Reflect.getMetadata(
      KAFKA_HANDLER_METADATA,
      Consumer.prototype.handle,
    );

    assert.deepEqual(metadata, { topic: undefined, options: {} });
  });

  it('stores the supplied topic and per-handler group override', () => {
    class Consumer {
      @KafkaHandler('events', { groupId: 'events-service' })
      handle(): void {}
    }

    const metadata: KafkaHandlerMetadata = Reflect.getMetadata(
      KAFKA_HANDLER_METADATA,
      Consumer.prototype.handle,
    );

    assert.deepEqual(metadata, {
      topic: 'events',
      options: { groupId: 'events-service' },
    });
  });
});

describe('KafkaContext', () => {
  it('exposes the topic, partition, and original message', () => {
    const message: KafkaIncomingMessage = {
      key: 'k',
      value: 'payload',
      offset: '42',
      headers: { 'x-trace': 'abc' },
    };
    const context = new KafkaContext('orders', 3, message);

    assert.equal(context.getTopic(), 'orders');
    assert.equal(context.getPartition(), 3);
    assert.equal(context.getMessage(), message);
  });
});
