import assert from 'node:assert/strict';
import Module from 'node:module';
import { afterEach, describe, it } from 'node:test';
import { createConfluentDriver, KafkaDriverProducer } from '../driver';

type ModuleLoad = (
  request: string,
  parent: unknown,
  isMain: boolean,
) => unknown;

interface ModuleWithLoad {
  _load: ModuleLoad;
}

const moduleWithLoad = Module as unknown as ModuleWithLoad;
const realLoad = moduleWithLoad._load;

/**
 * Intercept `require('@confluentinc/kafka-javascript')` so the driver can be
 * exercised without installing the optional native peer. Every other require
 * falls through to the real loader.
 */
function stubConfluentModule(replacement: unknown | (() => never)): void {
  moduleWithLoad._load = (request, parent, isMain) => {
    if (request === '@confluentinc/kafka-javascript') {
      if (typeof replacement === 'function') {
        return (replacement as () => never)();
      }
      return replacement;
    }
    return realLoad(request, parent, isMain);
  };
}

describe('createConfluentDriver', () => {
  afterEach(() => {
    moduleWithLoad._load = realLoad;
  });

  it('builds a driver that forwards client and producer config to the Confluent client', () => {
    const constructorConfigs: unknown[] = [];
    const producerConfigs: unknown[] = [];
    const fakeProducer = {} as KafkaDriverProducer;

    class FakeKafka {
      constructor(config?: unknown) {
        constructorConfigs.push(config);
      }

      producer(config?: unknown): KafkaDriverProducer {
        producerConfigs.push(config);
        return fakeProducer;
      }
    }

    stubConfluentModule({ KafkaJS: { Kafka: FakeKafka } });

    const driver = createConfluentDriver(
      { brokers: ['localhost:9092'], clientId: 'orders' },
      { allowAutoTopicCreation: false },
    );
    const created = driver.createProducer();

    assert.equal(created, fakeProducer);
    assert.deepEqual(constructorConfigs, [
      { kafkaJS: { brokers: ['localhost:9092'], clientId: 'orders' } },
    ]);
    assert.deepEqual(producerConfigs, [
      { kafkaJS: { allowAutoTopicCreation: false } },
    ]);
  });

  it('throws a descriptive error when the optional peer is not installed', () => {
    const cause = new Error('Cannot find module');
    stubConfluentModule(() => {
      throw cause;
    });

    assert.throws(
      () => createConfluentDriver({ brokers: [] }, {}),
      (error: unknown) => {
        assert.ok(error instanceof Error);
        assert.match(error.message, /@confluentinc\/kafka-javascript/);
        assert.match(error.message, /driverFactory/);
        assert.equal((error as { cause?: unknown }).cause, cause);
        return true;
      },
    );
  });
});
