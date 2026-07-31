import assert from 'node:assert/strict';
import Module from 'node:module';
import { afterEach, describe, it } from 'node:test';
import {
  createConfluentDriver,
  KafkaDriverConsumer,
  KafkaDriverProducer,
} from '../driver';

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
    const consumerConfigs: unknown[] = [];
    const fakeProducer = {} as KafkaDriverProducer;
    const fakeConsumer = {} as KafkaDriverConsumer;

    class FakeKafka {
      constructor(config?: unknown) {
        constructorConfigs.push(config);
      }

      producer(config?: unknown): KafkaDriverProducer {
        producerConfigs.push(config);
        return fakeProducer;
      }

      consumer(config?: unknown): KafkaDriverConsumer {
        consumerConfigs.push(config);
        return fakeConsumer;
      }
    }

    stubConfluentModule({ KafkaJS: { Kafka: FakeKafka } });

    const driver = createConfluentDriver(
      { brokers: ['localhost:9092'], clientId: 'orders' },
      { allowAutoTopicCreation: false },
    );
    const created = driver.createProducer();
    const consumerWithGroup = driver.createConsumer({ groupId: 'orders-svc' });
    const consumerDefault = driver.createConsumer();

    assert.equal(created, fakeProducer);
    assert.equal(consumerWithGroup, fakeConsumer);
    assert.equal(consumerDefault, fakeConsumer);
    assert.deepEqual(constructorConfigs, [
      { kafkaJS: { brokers: ['localhost:9092'], clientId: 'orders' } },
    ]);
    assert.deepEqual(producerConfigs, [
      { kafkaJS: { allowAutoTopicCreation: false } },
    ]);
    assert.deepEqual(consumerConfigs, [
      { kafkaJS: { groupId: 'orders-svc' } },
      { kafkaJS: {} },
    ]);
  });

  it('throws a descriptive error when the optional peer is not installed', () => {
    const cause = Object.assign(new Error('Cannot find module'), {
      code: 'MODULE_NOT_FOUND',
    });
    stubConfluentModule(() => {
      throw cause;
    });

    assert.throws(
      () => createConfluentDriver({ brokers: [] }, {}),
      (error: unknown) => {
        assert.ok(error instanceof Error);
        assert.match(error.message, /@confluentinc\/kafka-javascript/);
        assert.match(error.message, /is not installed/);
        assert.match(error.message, /driverFactory/);
        assert.equal((error as { cause?: unknown }).cause, cause);
        return true;
      },
    );
  });

  // The driver is a native addon: a binary built for another Node.js major
  // throws ERR_DLOPEN_FAILED even though the package IS installed. Reporting
  // that as "not installed" sends people hunting a dependency problem they do
  // not have, so the two cases must read differently.
  it('reports a native-binary load failure as installed-but-broken, not missing', () => {
    const cause = Object.assign(
      new Error(
        "The module '/app/node_modules/@confluentinc/kafka-javascript/build/Release/confluent-kafka-javascript.node'\nwas compiled against a different Node.js version using\nNODE_MODULE_VERSION 127.",
      ),
      { code: 'ERR_DLOPEN_FAILED' },
    );
    stubConfluentModule(() => {
      throw cause;
    });

    assert.throws(
      () => createConfluentDriver({ brokers: [] }, {}),
      (error: unknown) => {
        assert.ok(error instanceof Error);
        assert.match(error.message, /installed but failed to load/);
        assert.doesNotMatch(error.message, /is not installed/);
        // The first line of the underlying error is quoted, so the reader sees
        // the actual dlopen complaint without the multi-line noise.
        assert.match(error.message, /compiled against a different Node/);
        // The whole cause is kept (newlines collapsed) — the diagnosis lives
        // after the first line, so truncating there would hide it.
        assert.match(error.message, /NODE_MODULE_VERSION 127/);
        assert.doesNotMatch(error.message, /\n/);
        assert.match(error.message, /npm rebuild/);
        assert.match(error.message, /driverFactory/);
        assert.equal((error as { cause?: unknown }).cause, cause);
        return true;
      },
    );
  });

  it('describes a non-Error throw without assuming it is missing', () => {
    stubConfluentModule(() => {
      throw 'boom';
    });

    assert.throws(
      () => createConfluentDriver({ brokers: [] }, {}),
      (error: unknown) => {
        assert.ok(error instanceof Error);
        assert.match(error.message, /installed but failed to load: boom/);
        return true;
      },
    );
  });
});
