import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { Test } from '@nestjs/testing';
import {
  KafkaClientDriver,
  KafkaDriverAdmin,
  KafkaDriverProducer,
} from '../driver';
import {
  DEFAULT_KAFKA_HEALTH_TIMEOUT_MS,
  KafkaHealthIndicator,
} from '../kafka-health.indicator';
import { KafkaProducerService } from '../kafka-producer.service';
import { KafkaTestModule } from '../testing/kafka-test.module';

interface AdminScript {
  connect?: () => Promise<void>;
  listTopics?: () => Promise<string[]>;
  disconnect?: () => Promise<void>;
}

/** A driver whose admin clients follow `scripts`, one per created client. */
function scriptedDriver(...scripts: AdminScript[]) {
  const created: { script: AdminScript; disconnected: number; listed: number }[] = [];
  const driver: KafkaClientDriver = {
    createProducer: () => ({}) as KafkaDriverProducer,
    createConsumer: () => {
      throw new Error('not used');
    },
    createAdmin: () => {
      const script = scripts[Math.min(created.length, scripts.length - 1)];
      const record = { script, disconnected: 0, listed: 0 };
      created.push(record);
      const admin: KafkaDriverAdmin = {
        connect: script.connect ?? (async () => {}),
        listTopics: async () => {
          record.listed += 1;
          return (script.listTopics ?? (async () => ['orders', 'payments']))();
        },
        disconnect: async () => {
          record.disconnected += 1;
          await script.disconnect?.();
        },
      };
      return admin;
    },
  };
  return { driver, created };
}

describe('KafkaHealthIndicator', () => {
  it('reports the cluster up after a metadata round trip', async () => {
    const { driver } = scriptedDriver({});
    const indicator = new KafkaHealthIndicator(driver);

    const result = await indicator.isHealthy();
    assert.equal(result.kafka.status, 'up');
    assert.equal(result.kafka.topics, 2);
    assert.equal(typeof result.kafka.latencyMs, 'number');

    assert.deepEqual(Object.keys(await indicator.isHealthy('broker')), ['broker']);
  });

  it('reuses one admin client while the checks succeed', async () => {
    const { driver, created } = scriptedDriver({});
    const indicator = new KafkaHealthIndicator(driver);

    await indicator.isHealthy();
    await indicator.isHealthy();
    assert.equal(created.length, 1);
    assert.equal(created[0].listed, 2);
  });

  it('reports the cluster down, then checks with a fresh admin client', async () => {
    const { driver, created } = scriptedDriver(
      { listTopics: async () => Promise.reject(new Error('Local: Broker transport failure')) },
      {},
    );
    const indicator = new KafkaHealthIndicator(driver);

    const down = await indicator.isHealthy();
    assert.deepEqual(down, {
      kafka: { status: 'down', message: 'Local: Broker transport failure' },
    });

    // A client that failed while the cluster was down keeps reporting it, so
    // the failed one is released and the next check starts from a new one.
    const up = await indicator.isHealthy();
    assert.equal(up.kafka.status, 'up');
    assert.equal(created.length, 2);
    await new Promise(resolve => setImmediate(resolve));
    assert.equal(created[0].disconnected, 1, 'the failed client was released');
  });

  it('reports a client that cannot even connect', async () => {
    const { driver } = scriptedDriver({
      connect: async () => Promise.reject('connection refused'),
    });
    const result = await new KafkaHealthIndicator(driver).isHealthy();
    assert.deepEqual(result, { kafka: { status: 'down', message: 'connection refused' } });
  });

  it('gives up on a round trip that takes longer than the timeout', async () => {
    const { driver } = scriptedDriver({ listTopics: () => new Promise(() => {}) });
    const started = Date.now();

    const result = await new KafkaHealthIndicator(driver).isHealthy('kafka', {
      timeoutMs: 30,
    });
    assert.deepEqual(result, {
      kafka: { status: 'down', message: 'no cluster metadata within 30 ms' },
    });
    assert.ok(Date.now() - started < 1_000);
    assert.equal(DEFAULT_KAFKA_HEALTH_TIMEOUT_MS, 5_000);
  });

  it('shares one round trip between overlapping checks', async () => {
    let release: (topics: string[]) => void = () => {};
    const { driver, created } = scriptedDriver({
      listTopics: () => new Promise(resolve => (release = resolve)),
    });
    const indicator = new KafkaHealthIndicator(driver);

    const first = indicator.isHealthy('a');
    const second = indicator.isHealthy('b');
    await new Promise(resolve => setImmediate(resolve));
    release(['orders']);

    assert.equal((await first).a.status, 'up');
    assert.equal((await second).b.status, 'up');
    assert.equal(created[0].listed, 1);
  });

  it('reports the cluster down when the driver cannot open an admin client', async () => {
    const driver: KafkaClientDriver = {
      createProducer: () => ({}) as KafkaDriverProducer,
      createConsumer: () => {
        throw new Error('not used');
      },
    };
    const result = await new KafkaHealthIndicator(driver).isHealthy();
    assert.equal(result.kafka.status, 'down');
    assert.match(String(result.kafka.message), /cannot open an admin client/);
  });

  it('releases its admin client on shutdown', async () => {
    const { driver, created } = scriptedDriver({});
    const indicator = new KafkaHealthIndicator(driver);

    await indicator.onApplicationShutdown(); // nothing opened yet: a no-op
    await indicator.isHealthy();
    await indicator.onApplicationShutdown();
    assert.equal(created[0].disconnected, 1);
  });

  it('shuts down cleanly even when the admin client fails to disconnect', async () => {
    const { driver } = scriptedDriver({
      disconnect: async () => Promise.reject(new Error('already gone')),
    });
    const indicator = new KafkaHealthIndicator(driver);

    await indicator.isHealthy();
    await assert.doesNotReject(indicator.onApplicationShutdown());
  });

  it('is provided by the module and reports the in-memory cluster up', async () => {
    const moduleRef = await Test.createTestingModule({
      imports: [KafkaTestModule.forRoot()],
    }).compile();
    const app = await moduleRef.init();

    await app.get(KafkaProducerService).send({
      topic: 'orders',
      messages: [{ value: '1' }],
    });
    const result = await app.get(KafkaHealthIndicator).isHealthy();
    assert.equal(result.kafka.status, 'up');
    assert.equal(result.kafka.topics, 1);

    await app.close();
  });
});
