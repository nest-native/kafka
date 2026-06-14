import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { BadRequestException, Injectable, Logger } from '@nestjs/common';
import { Test } from '@nestjs/testing';
import {
  KafkaClientDriver,
  KafkaConsumerBatch,
  KafkaConsumerConfig,
  KafkaConsumerMessage,
  KafkaConsumerRunConfig,
  KafkaDriverConsumer,
  KafkaDriverFactory,
  KafkaDriverProducer,
  KafkaEachBatchHandler,
  KafkaEachMessageHandler,
} from '../driver';
import { KafkaConsumer } from '../kafka-consumer.decorator';
import { KafkaHandler } from '../kafka-handler.decorator';
import { KafkaBatch, KafkaMessage } from '../kafka-params.decorators';
import { KafkaBatchContext } from '../kafka-context';
import { KafkaModule } from '../kafka.module';

/** Silence Nest's bootstrap logging during the tests. */
Logger.overrideLogger(false);

interface RecordedConsumer {
  config: KafkaConsumerConfig;
  runConfig?: KafkaConsumerRunConfig;
  eachMessage?: KafkaEachMessageHandler;
  eachBatch?: KafkaEachBatchHandler;
  subscribedTopics: string[];
  disconnected: number;
}

interface ControllableDriver {
  factory: KafkaDriverFactory;
  consumers: RecordedConsumer[];
}

function noopProducer(): KafkaDriverProducer {
  return {
    connect: async () => {},
    disconnect: async () => {},
    send: async () => [],
    sendBatch: async () => [],
    transaction: async () => ({
      send: async () => [],
      sendBatch: async () => [],
      sendOffsets: async () => {},
      commit: async () => {},
      abort: async () => {},
    }),
  };
}

function createControllableDriver(): ControllableDriver {
  const consumers: RecordedConsumer[] = [];

  const driver: KafkaClientDriver = {
    createProducer: noopProducer,
    createConsumer: (config = {}) => {
      const record: RecordedConsumer = {
        config,
        subscribedTopics: [],
        disconnected: 0,
      };
      const consumer: KafkaDriverConsumer = {
        connect: async () => {},
        disconnect: async () => {
          record.disconnected += 1;
        },
        subscribe: async subscription => {
          record.subscribedTopics.push(...subscription.topics);
        },
        run: async runConfig => {
          record.runConfig = runConfig;
          record.eachMessage = runConfig.eachMessage;
          record.eachBatch = runConfig.eachBatch;
        },
      };
      consumers.push(record);
      return consumer;
    },
  };

  return { factory: () => driver, consumers };
}

function messages(...values: unknown[]): KafkaConsumerMessage[] {
  return values.map((value, index) => ({
    value: typeof value === 'string' ? value : JSON.stringify(value),
    offset: String(index),
  }));
}

function batch(
  topic: string,
  partition: number,
  msgs: KafkaConsumerMessage[],
): KafkaConsumerBatch {
  return { topic, partition, messages: msgs };
}

/** Poll a condition across event-loop turns until it holds (or time runs out). */
async function waitFor(
  predicate: () => boolean,
  timeoutMs = 1000,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() > deadline) {
      throw new Error('waitFor timed out');
    }
    await new Promise(resolve => setTimeout(resolve, 1));
  }
}

describe('Kafka batch consumption', () => {
  it('invokes a batch handler once per batch with the array of payloads', async () => {
    const driver = createControllableDriver();
    const seen: { payloads: unknown[]; topic: string; partition: number }[] = [];

    @KafkaConsumer('metrics', { groupId: 'metrics-batch' })
    class MetricsConsumer {
      @KafkaHandler(undefined, { batch: true })
      consume(
        @KafkaMessage() payloads: unknown[],
        @KafkaBatch() raw: KafkaConsumerBatch,
      ): void {
        seen.push({
          payloads,
          topic: raw.topic,
          partition: raw.partition,
        });
      }
    }

    const moduleRef = await Test.createTestingModule({
      imports: [
        KafkaModule.forRoot({ driverFactory: driver.factory }),
        KafkaModule.forFeature([MetricsConsumer]),
      ],
    }).compile();
    const app = await moduleRef.init();

    assert.equal(driver.consumers.length, 1);
    const consumer = driver.consumers[0];
    assert.ok(consumer.eachBatch, 'a batch handler runs eachBatch');
    assert.equal(consumer.eachMessage, undefined);
    assert.equal(consumer.runConfig?.eachBatchAutoResolve, false);

    const resolved: string[] = [];
    await consumer.eachBatch?.({
      batch: batch('metrics', 4, messages({ n: 1 }, { n: 2 }, { n: 3 })),
      resolveOffset: offset => resolved.push(offset),
    });

    assert.equal(seen.length, 1, 'one invocation for the whole batch');
    assert.deepEqual(seen[0].payloads, [{ n: 1 }, { n: 2 }, { n: 3 }]);
    assert.equal(seen[0].topic, 'metrics');
    assert.equal(seen[0].partition, 4);
    // Every message offset resolved, so a rebalance keeps the progress made.
    assert.deepEqual(resolved, ['0', '1', '2']);

    await app.close();
    assert.equal(consumer.disconnected, 1);
  });

  it('exposes the batch context through @KafkaCtx via KafkaBatchContext', async () => {
    const driver = createControllableDriver();
    let context: KafkaBatchContext | undefined;

    @KafkaConsumer('events')
    class EventsConsumer {
      @KafkaHandler('events', { batch: true })
      consume(
        _payloads: unknown[],
        ctx: KafkaBatchContext,
      ): void {
        context = ctx;
      }
    }

    const moduleRef = await Test.createTestingModule({
      imports: [
        KafkaModule.forRoot({ driverFactory: driver.factory }),
        KafkaModule.forFeature([EventsConsumer]),
      ],
    }).compile();
    const app = await moduleRef.init();

    const events = messages({ id: 'a' }, { id: 'b' });
    await driver.consumers[0].eachBatch?.({
      batch: batch('events', 2, events),
      resolveOffset: () => {},
    });

    assert.ok(context instanceof KafkaBatchContext);
    assert.equal(context?.getTopic(), 'events');
    assert.equal(context?.getPartition(), 2);
    assert.deepEqual(context?.getBatch().messages, events);

    await app.close();
  });

  it('does not resolve an offset for a message that carries none', async () => {
    const driver = createControllableDriver();
    const seen: unknown[][] = [];

    @KafkaConsumer('partial')
    class PartialConsumer {
      @KafkaHandler('partial', { batch: true })
      consume(@KafkaMessage() payloads: unknown[]): void {
        seen.push(payloads);
      }
    }

    const moduleRef = await Test.createTestingModule({
      imports: [
        KafkaModule.forRoot({ driverFactory: driver.factory }),
        KafkaModule.forFeature([PartialConsumer]),
      ],
    }).compile();
    const app = await moduleRef.init();

    const resolved: string[] = [];
    await driver.consumers[0].eachBatch?.({
      batch: {
        topic: 'partial',
        partition: 0,
        messages: [
          { value: JSON.stringify({ id: 1 }), offset: '10' },
          { value: JSON.stringify({ id: 2 }) }, // no offset
        ],
      },
      resolveOffset: offset => resolved.push(offset),
    });

    assert.deepEqual(seen, [[{ id: 1 }, { id: 2 }]]);
    // Only the message with an offset is resolved.
    assert.deepEqual(resolved, ['10']);

    await app.close();
  });

  it('ignores a batch delivered for an unrouted topic', async () => {
    const driver = createControllableDriver();
    let calls = 0;

    @KafkaConsumer('known')
    class KnownConsumer {
      @KafkaHandler('known', { batch: true })
      consume(): void {
        calls += 1;
      }
    }

    const moduleRef = await Test.createTestingModule({
      imports: [
        KafkaModule.forRoot({ driverFactory: driver.factory }),
        KafkaModule.forFeature([KnownConsumer]),
      ],
    }).compile();
    const app = await moduleRef.init();

    await driver.consumers[0].eachBatch?.({
      batch: batch('elsewhere', 0, messages({ id: 1 })),
      resolveOffset: () => {},
    });
    assert.equal(calls, 0);

    await app.close();
  });

  it('maps an unhandled batch error through the error mapper', async () => {
    const driver = createControllableDriver();

    @KafkaConsumer('explode')
    class ExplodingBatchConsumer {
      @KafkaHandler('explode', { batch: true })
      consume(): void {
        throw new Error('batch downstream timeout');
      }
    }

    @KafkaConsumer('bad')
    class BadBatchConsumer {
      @KafkaHandler('bad', { batch: true })
      consume(): void {
        throw new BadRequestException('bad batch');
      }
    }

    const moduleRef = await Test.createTestingModule({
      imports: [
        KafkaModule.forRoot({ driverFactory: driver.factory }),
        KafkaModule.forFeature([ExplodingBatchConsumer, BadBatchConsumer]),
      ],
    }).compile();
    const app = await moduleRef.init();

    const exploding = driver.consumers.find(c =>
      c.subscribedTopics.includes('explode'),
    );
    const bad = driver.consumers.find(c => c.subscribedTopics.includes('bad'));

    // A plain Error retries → rethrows so the offset stays uncommitted.
    await assert.rejects(
      exploding?.eachBatch?.({
        batch: batch('explode', 0, messages({ id: 1 })),
        resolveOffset: () => {},
      }) ?? Promise.resolve(),
      /batch downstream timeout/,
    );

    // A 4xx commits → resolves cleanly.
    await assert.doesNotReject(
      bad?.eachBatch?.({
        batch: batch('bad', 0, messages({ id: 1 })),
        resolveOffset: () => {},
      }) ?? Promise.resolve(),
    );

    await app.close();
  });

  it('runs per-message and batch handlers of the same group on separate consumers', async () => {
    const driver = createControllableDriver();

    @KafkaConsumer(undefined, { groupId: 'mixed' })
    class MixedConsumer {
      @KafkaHandler('single')
      one(): void {}

      @KafkaHandler('many', { batch: true })
      many(): void {}
    }

    const moduleRef = await Test.createTestingModule({
      imports: [
        KafkaModule.forRoot({ driverFactory: driver.factory }),
        KafkaModule.forFeature([MixedConsumer]),
      ],
    }).compile();
    const app = await moduleRef.init();

    // Same group, but two consumers: one eachMessage, one eachBatch.
    assert.equal(driver.consumers.length, 2);
    const messageConsumer = driver.consumers.find(c => c.eachMessage);
    const batchConsumer = driver.consumers.find(c => c.eachBatch);
    assert.ok(messageConsumer);
    assert.ok(batchConsumer);
    assert.deepEqual(messageConsumer?.subscribedTopics, ['single']);
    assert.deepEqual(batchConsumer?.subscribedTopics, ['many']);

    await app.close();
  });
});

describe('Kafka per-topic concurrency (nestjs/nest#12703)', () => {
  it('defaults partitionsConsumedConcurrently to 1 (ordered)', async () => {
    const driver = createControllableDriver();

    @KafkaConsumer('ordered')
    class OrderedConsumer {
      @KafkaHandler()
      handle(): void {}
    }

    const moduleRef = await Test.createTestingModule({
      imports: [
        KafkaModule.forRoot({ driverFactory: driver.factory }),
        KafkaModule.forFeature([OrderedConsumer]),
      ],
    }).compile();
    const app = await moduleRef.init();

    assert.equal(
      driver.consumers[0].runConfig?.partitionsConsumedConcurrently,
      1,
    );

    await app.close();
  });

  it('lets a handler opt into higher partition concurrency', async () => {
    const driver = createControllableDriver();

    @KafkaConsumer('fast', { concurrency: 4 })
    class FastConsumer {
      @KafkaHandler()
      handle(): void {}
    }

    const moduleRef = await Test.createTestingModule({
      imports: [
        KafkaModule.forRoot({ driverFactory: driver.factory }),
        KafkaModule.forFeature([FastConsumer]),
      ],
    }).compile();
    const app = await moduleRef.init();

    assert.equal(
      driver.consumers[0].runConfig?.partitionsConsumedConcurrently,
      4,
    );

    await app.close();
  });

  it('resolves concurrency handler → consumer → module default', async () => {
    const driver = createControllableDriver();

    // Module default 2, consumer raises to 5, handler overrides down to 3.
    @KafkaConsumer('layered', { concurrency: 5 })
    class LayeredConsumer {
      @KafkaHandler('layered', { concurrency: 3 })
      handle(): void {}
    }

    @KafkaConsumer('inherits', { groupId: 'inherits' })
    class InheritsModuleDefault {
      @KafkaHandler()
      handle(): void {}
    }

    const moduleRef = await Test.createTestingModule({
      imports: [
        KafkaModule.forRoot({ driverFactory: driver.factory, concurrency: 2 }),
        KafkaModule.forFeature([LayeredConsumer, InheritsModuleDefault]),
      ],
    }).compile();
    const app = await moduleRef.init();

    const layered = driver.consumers.find(c =>
      c.subscribedTopics.includes('layered'),
    );
    const inherits = driver.consumers.find(c =>
      c.subscribedTopics.includes('inherits'),
    );
    assert.equal(layered?.runConfig?.partitionsConsumedConcurrently, 3);
    assert.equal(inherits?.runConfig?.partitionsConsumedConcurrently, 2);

    await app.close();
  });
});

describe('Kafka backpressure (in-flight cap)', () => {
  it('caps concurrent in-flight messages at maxInFlight', async () => {
    const driver = createControllableDriver();
    let running = 0;
    let peak = 0;
    const release: (() => void)[] = [];

    @KafkaConsumer('throttled', { maxInFlight: 2 })
    class ThrottledConsumer {
      @KafkaHandler()
      async handle(): Promise<void> {
        running += 1;
        peak = Math.max(peak, running);
        await new Promise<void>(resolve => release.push(resolve));
        running -= 1;
      }
    }

    const moduleRef = await Test.createTestingModule({
      imports: [
        KafkaModule.forRoot({ driverFactory: driver.factory }),
        KafkaModule.forFeature([ThrottledConsumer]),
      ],
    }).compile();
    const app = await moduleRef.init();
    const each = driver.consumers[0].eachMessage;
    assert.ok(each);

    // Deliver four messages without awaiting; only two may run at once. Handler
    // dispatch resolves the instance asynchronously, so wait until the cap is
    // actually reached (two handlers parked) before asserting.
    const inFlight = [0, 1, 2, 3].map(id =>
      each({ topic: 'throttled', partition: 0, message: { value: String(id) } }),
    );

    await waitFor(() => release.length === 2);
    assert.equal(peak, 2, 'never more than maxInFlight handlers running');

    // Release the first two; the next two enter, still capped at two.
    release.shift()?.();
    release.shift()?.();
    await waitFor(() => release.length === 2);
    assert.equal(peak, 2);

    release.forEach(fn => fn());
    await Promise.all(inFlight);
    assert.equal(peak, 2);

    await app.close();
  });

  it('drains in-flight batch work before disconnecting on shutdown', async () => {
    const driver = createControllableDriver();
    let release: () => void = () => {};
    const gate = new Promise<void>(resolve => {
      release = resolve;
    });
    let completed = false;

    @KafkaConsumer('slow-batch')
    class SlowBatchConsumer {
      @KafkaHandler('slow-batch', { batch: true })
      async consume(): Promise<void> {
        await gate;
        completed = true;
      }
    }

    const moduleRef = await Test.createTestingModule({
      imports: [
        KafkaModule.forRoot({ driverFactory: driver.factory }),
        KafkaModule.forFeature([SlowBatchConsumer]),
      ],
    }).compile();
    const app = await moduleRef.init();
    const each = driver.consumers[0].eachBatch;
    assert.ok(each);

    const inFlight = each({
      batch: batch('slow-batch', 0, messages({ id: 1 })),
      resolveOffset: () => {},
    });

    const closing = app.close();
    release();
    await closing;
    await inFlight;

    assert.equal(completed, true);
    assert.equal(driver.consumers[0].disconnected, 1);
  });

  it('stops accepting new batch claims once shutdown has begun', async () => {
    const driver = createControllableDriver();
    const handled: number[] = [];

    @KafkaConsumer('gated-batch')
    class GatedBatchConsumer {
      @KafkaHandler('gated-batch', { batch: true })
      consume(@KafkaMessage() payloads: { id: number }[]): void {
        handled.push(...payloads.map(p => p.id));
      }
    }

    const moduleRef = await Test.createTestingModule({
      imports: [
        KafkaModule.forRoot({ driverFactory: driver.factory }),
        KafkaModule.forFeature([GatedBatchConsumer]),
      ],
    }).compile();
    const app = await moduleRef.init();
    const each = driver.consumers[0].eachBatch;
    assert.ok(each);

    await each({
      batch: batch('gated-batch', 0, messages({ id: 1 })),
      resolveOffset: () => {},
    });
    await app.close();

    // A batch delivered after shutdown began is refused.
    await each({
      batch: batch('gated-batch', 0, messages({ id: 2 })),
      resolveOffset: () => {},
    });

    assert.deepEqual(handled, [1]);
  });
});
