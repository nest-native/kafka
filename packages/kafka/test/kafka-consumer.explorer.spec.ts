import assert from 'node:assert/strict';
import { afterEach, describe, it } from 'node:test';
import {
  ArgumentsHost,
  BadRequestException,
  CallHandler,
  CanActivate,
  Catch,
  ExceptionFilter,
  ExecutionContext,
  ForbiddenException,
  Injectable,
  Logger,
  NestInterceptor,
  PipeTransform,
  Scope,
  Type,
  UnauthorizedException,
  UseFilters,
  UseGuards,
  UseInterceptors,
  UsePipes,
} from '@nestjs/common';
import { APP_GUARD, APP_INTERCEPTOR } from '@nestjs/core';
import { Test } from '@nestjs/testing';
import { Observable, map } from 'rxjs';
import {
  KafkaClientDriver,
  KafkaConsumerConfig,
  KafkaDriverConsumer,
  KafkaDriverFactory,
  KafkaDriverProducer,
  KafkaEachBatchHandler,
  KafkaEachMessageHandler,
  KafkaEachMessagePayload,
  KafkaSubscription,
  KafkaTopicPartitions,
} from '../driver';
import { KafkaConsumer } from '../kafka-consumer.decorator';
import { KafkaHandler } from '../kafka-handler.decorator';
import { KafkaContext } from '../kafka-context';
import { KafkaModule } from '../kafka.module';
import { KafkaModuleOptions } from '../interfaces';
import { KafkaErrorBehavior, KafkaErrorMapper } from '../kafka-error-mapping';

/** Silence Nest's bootstrap logging during the tests. */
Logger.overrideLogger(false);

interface RecordedConsumer {
  config: KafkaConsumerConfig;
  consumer: KafkaDriverConsumer;
  subscriptions: KafkaSubscription[];
  eachMessage?: KafkaEachMessageHandler;
  eachBatch?: KafkaEachBatchHandler;
  connected: number;
  disconnected: number;
  /** `pause topic[partitions]` / `resume …`, for a `pausable` driver. */
  pauses: string[];
}

interface ControllableDriver {
  factory: KafkaDriverFactory;
  consumers: RecordedConsumer[];
  emit: (payload: KafkaEachMessagePayload) => Promise<void>;
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

function createControllableDriver({ pausable = false } = {}): ControllableDriver {
  const consumers: RecordedConsumer[] = [];
  const describePartitions = (topics: KafkaTopicPartitions[]): string =>
    topics
      .map(({ topic, partitions }) => `${topic}[${partitions ?? '*'}]`)
      .join(',');

  const driver: KafkaClientDriver = {
    createProducer: noopProducer,
    createConsumer: (config = {}) => {
      const record: RecordedConsumer = {
        config,
        subscriptions: [],
        connected: 0,
        disconnected: 0,
        pauses: [],
        consumer: undefined as unknown as KafkaDriverConsumer,
      };
      record.consumer = {
        connect: async () => {
          record.connected += 1;
        },
        disconnect: async () => {
          record.disconnected += 1;
        },
        subscribe: async subscription => {
          record.subscriptions.push(subscription);
        },
        run: async runConfig => {
          record.eachMessage = runConfig.eachMessage;
          record.eachBatch = runConfig.eachBatch;
        },
      };
      if (pausable) {
        record.consumer.pause = topics => {
          record.pauses.push(`pause ${describePartitions(topics)}`);
        };
        record.consumer.resume = topics => {
          record.pauses.push(`resume ${describePartitions(topics)}`);
        };
      }
      consumers.push(record);
      return record.consumer;
    },
  };

  // Deliver to every running consumer, mirroring a broker that hands a fetched
  // record to the consumer's `eachMessage` callback. The explorer is responsible
  // for routing the topic to the right handler (or ignoring it).
  const emit = async (payload: KafkaEachMessagePayload): Promise<void> => {
    for (const record of consumers) {
      if (record.eachMessage) {
        await record.eachMessage(payload);
      }
    }
  };

  return { factory: () => driver, consumers, emit };
}

function messagePayload(
  topic: string,
  value: unknown,
  partition = 0,
): KafkaEachMessagePayload {
  const encoded =
    value === null || typeof value === 'string'
      ? (value as string | null)
      : JSON.stringify(value);
  return {
    topic,
    partition,
    message: { value: encoded, offset: '0' },
  };
}

afterEach(() => {});

describe('Kafka consumer transport', () => {
  it('routes a message to a matching handler through the driver', async () => {
    const driver = createControllableDriver();
    const received: unknown[] = [];

    @KafkaConsumer('orders', { groupId: 'orders-service' })
    class OrdersConsumer {
      @KafkaHandler()
      handle(payload: unknown): void {
        received.push(payload);
      }
    }

    const moduleRef = await Test.createTestingModule({
      imports: [
        KafkaModule.forRoot({ driverFactory: driver.factory }),
        KafkaModule.forFeature([OrdersConsumer]),
      ],
    }).compile();
    const app = await moduleRef.init();

    assert.equal(driver.consumers.length, 1);
    assert.equal(driver.consumers[0].config.groupId, 'orders-service');
    assert.equal(driver.consumers[0].connected, 1);
    assert.deepEqual(driver.consumers[0].subscriptions, [
      { topics: ['orders'] },
    ]);

    await driver.emit(messagePayload('orders', { id: 'a' }));

    assert.deepEqual(received, [{ id: 'a' }]);

    await app.close();
    assert.equal(driver.consumers[0].disconnected, 1);
  });

  it('does nothing when no consumers are registered', async () => {
    const driver = createControllableDriver();

    const moduleRef = await Test.createTestingModule({
      imports: [KafkaModule.forRoot({ driverFactory: driver.factory })],
    }).compile();
    const app = await moduleRef.init();

    assert.equal(driver.consumers.length, 0);

    await app.close();
  });

  it('runs the full enhancer pipeline: guard, interceptor, pipe, filter', async () => {
    const driver = createControllableDriver();
    const order: string[] = [];

    @Injectable()
    class AllowGuard implements CanActivate {
      canActivate(context: ExecutionContext): boolean {
        order.push(`guard:${context.getType()}`);
        return true;
      }
    }

    @Injectable()
    class TraceInterceptor implements NestInterceptor {
      intercept(_context: ExecutionContext, next: CallHandler): Observable<unknown> {
        order.push('interceptor:before');
        return next.handle().pipe(
          map(value => {
            order.push('interceptor:after');
            return value;
          }),
        );
      }
    }

    @Injectable()
    class DoublingPipe implements PipeTransform {
      transform(value: { n: number }): { n: number } {
        order.push('pipe');
        return { n: value.n * 2 };
      }
    }

    @Catch()
    class RecordingFilter implements ExceptionFilter {
      catch(exception: unknown, _host: ArgumentsHost): void {
        order.push(`filter:${(exception as Error).message}`);
      }
    }

    @KafkaConsumer('events')
    @UseGuards(AllowGuard)
    @UseInterceptors(TraceInterceptor)
    class EventsConsumer {
      @KafkaHandler()
      @UsePipes(DoublingPipe)
      @UseFilters(RecordingFilter)
      handle(payload: { n: number }): void {
        order.push(`handler:${payload.n}`);
      }
    }

    const moduleRef = await Test.createTestingModule({
      imports: [
        KafkaModule.forRoot({ driverFactory: driver.factory }),
        KafkaModule.forFeature([EventsConsumer]),
      ],
    }).compile();
    const app = await moduleRef.init();

    await driver.emit(messagePayload('events', { n: 21 }));

    assert.deepEqual(order, [
      'guard:rpc',
      'interceptor:before',
      'pipe',
      'handler:42',
      'interceptor:after',
    ]);

    await app.close();
  });

  it('blocks the handler when a guard denies access', async () => {
    const driver = createControllableDriver();
    let handled = false;

    @Injectable()
    class DenyGuard implements CanActivate {
      canActivate(): boolean {
        return false;
      }
    }

    let caught: unknown;

    @Catch()
    class CaptureFilter implements ExceptionFilter {
      // Returning a value tells Nest the exception is handled, mirroring how an
      // RPC exception filter swallows the error and acknowledges the message.
      catch(exception: unknown): string {
        caught = exception;
        return 'handled';
      }
    }

    @KafkaConsumer('secured')
    class SecuredConsumer {
      @KafkaHandler()
      @UseGuards(DenyGuard)
      @UseFilters(CaptureFilter)
      handle(): void {
        handled = true;
      }
    }

    const moduleRef = await Test.createTestingModule({
      imports: [KafkaModule.forRoot({ driverFactory: driver.factory })],
      providers: [SecuredConsumer, DenyGuard, CaptureFilter],
    }).compile();
    const app = await moduleRef.init();

    await driver.emit(messagePayload('secured', { id: 'x' }));

    assert.equal(handled, false);
    assert.ok(caught instanceof ForbiddenException);

    await app.close();
  });

  it('lets a filter handle a thrown exception and supports global enhancers', async () => {
    const driver = createControllableDriver();
    const seen: string[] = [];

    @Injectable()
    class GlobalGuard implements CanActivate {
      canActivate(): boolean {
        seen.push('global-guard');
        return true;
      }
    }

    @Injectable()
    class GlobalInterceptor implements NestInterceptor {
      intercept(_context: ExecutionContext, next: CallHandler): Observable<unknown> {
        seen.push('global-interceptor');
        return next.handle();
      }
    }

    @Catch(BadRequestException)
    class BadRequestFilter implements ExceptionFilter {
      catch(exception: BadRequestException): string {
        seen.push(`filter:${exception.message}`);
        return 'handled';
      }
    }

    @KafkaConsumer('throwing')
    class ThrowingConsumer {
      @KafkaHandler()
      @UseFilters(BadRequestFilter)
      handle(): void {
        throw new BadRequestException('bad payload');
      }
    }

    const moduleRef = await Test.createTestingModule({
      imports: [KafkaModule.forRoot({ driverFactory: driver.factory })],
      providers: [
        ThrowingConsumer,
        BadRequestFilter,
        { provide: APP_GUARD, useClass: GlobalGuard },
        { provide: APP_INTERCEPTOR, useClass: GlobalInterceptor },
      ],
    }).compile();
    const app = await moduleRef.init();

    await driver.emit(messagePayload('throwing', { id: 'x' }));

    assert.deepEqual(seen, [
      'global-guard',
      'global-interceptor',
      'filter:bad payload',
    ]);

    await app.close();
  });

  it('retries (rethrows) an unhandled non-client error and commits a 4xx', async () => {
    const driver = createControllableDriver();

    // A bare Error is treated as transient by the default mapper, so it must
    // surface to the driver (offset uncommitted → redelivery).
    @KafkaConsumer('explode')
    class ExplodingConsumer {
      @KafkaHandler()
      handle(): void {
        throw new Error('downstream timeout');
      }
    }

    // A 4xx client error is non-retryable, so the default mapper commits it: the
    // explorer swallows the error instead of rethrowing.
    @KafkaConsumer('bad-payload')
    class BadPayloadConsumer {
      @KafkaHandler()
      handle(): void {
        throw new UnauthorizedException('nope');
      }
    }

    const moduleRef = await Test.createTestingModule({
      imports: [
        KafkaModule.forRoot({ driverFactory: driver.factory }),
        KafkaModule.forFeature([ExplodingConsumer, BadPayloadConsumer]),
      ],
    }).compile();
    const app = await moduleRef.init();

    await assert.rejects(
      driver.emit(messagePayload('explode', { id: 'x' })),
      /downstream timeout/,
    );

    // The 4xx error is committed, not rethrown.
    await driver.emit(messagePayload('bad-payload', { id: 'x' }));

    await app.close();
  });

  it('exposes the raw transport context to enhancers', async () => {
    const driver = createControllableDriver();
    let contextSnapshot: { topic: string; partition: number } | undefined;

    @Injectable()
    class ContextGuard implements CanActivate {
      canActivate(context: ExecutionContext): boolean {
        const kafkaContext = context.switchToRpc().getContext<KafkaContext>();
        contextSnapshot = {
          topic: kafkaContext.getTopic(),
          partition: kafkaContext.getPartition(),
        };
        return true;
      }
    }

    @KafkaConsumer('contextual')
    class ContextualConsumer {
      @KafkaHandler()
      @UseGuards(ContextGuard)
      handle(): void {}
    }

    const moduleRef = await Test.createTestingModule({
      imports: [
        KafkaModule.forRoot({ driverFactory: driver.factory }),
        KafkaModule.forFeature([ContextualConsumer]),
      ],
    }).compile();
    const app = await moduleRef.init();

    await driver.emit(messagePayload('contextual', { id: 'x' }, 3));

    assert.deepEqual(contextSnapshot, { topic: 'contextual', partition: 3 });

    await app.close();
  });

  it('groups handlers by consumer group and ignores unknown topics', async () => {
    const driver = createControllableDriver();
    const hits: string[] = [];

    @KafkaConsumer(undefined, { groupId: 'group-a' })
    class GroupAConsumer {
      @KafkaHandler('topic-a')
      a(): void {
        hits.push('a');
      }

      @KafkaHandler('topic-a2')
      a2(): void {
        hits.push('a2');
      }
    }

    @KafkaConsumer('topic-b', { groupId: 'group-b' })
    class GroupBConsumer {
      @KafkaHandler('topic-b', { groupId: 'group-b-override' })
      b(): void {
        hits.push('b');
      }
    }

    const moduleRef = await Test.createTestingModule({
      imports: [
        KafkaModule.forRoot({ driverFactory: driver.factory }),
        KafkaModule.forFeature([GroupAConsumer, GroupBConsumer]),
      ],
    }).compile();
    const app = await moduleRef.init();

    const groupIds = driver.consumers.map(c => c.config.groupId).sort();
    assert.deepEqual(groupIds, ['group-a', 'group-b-override']);

    await driver.emit(messagePayload('topic-a', { id: 1 }));
    await driver.emit(messagePayload('topic-b', { id: 2 }));
    await driver.emit(messagePayload('unknown-topic', { id: 3 }));

    assert.deepEqual(hits.sort(), ['a', 'b']);

    await app.close();
  });

  it('decodes string, JSON, and tombstone payloads', async () => {
    const driver = createControllableDriver();
    const received: unknown[] = [];

    @KafkaConsumer('values')
    class ValuesConsumer {
      @KafkaHandler()
      handle(payload: unknown): void {
        received.push(payload);
      }
    }

    const moduleRef = await Test.createTestingModule({
      imports: [
        KafkaModule.forRoot({ driverFactory: driver.factory }),
        KafkaModule.forFeature([ValuesConsumer]),
      ],
    }).compile();
    const app = await moduleRef.init();

    await driver.emit({
      topic: 'values',
      partition: 0,
      message: { value: Buffer.from(JSON.stringify({ id: 1 })) },
    });
    await driver.emit({
      topic: 'values',
      partition: 0,
      message: { value: 'plain text' },
    });
    await driver.emit({
      topic: 'values',
      partition: 0,
      message: { value: null },
    });

    assert.deepEqual(received, [{ id: 1 }, 'plain text', null]);

    await app.close();
  });

  it('throws at bootstrap when a handler has no resolvable topic', async () => {
    const driver = createControllableDriver();

    @KafkaConsumer()
    class UntopicedConsumer {
      @KafkaHandler()
      handle(): void {}
    }

    const moduleRef = await Test.createTestingModule({
      imports: [
        KafkaModule.forRoot({ driverFactory: driver.factory }),
        KafkaModule.forFeature([UntopicedConsumer]),
      ],
    }).compile();
    await assert.rejects(moduleRef.init(), /has no topic/);
  });

  it('resolves a fresh request-scoped consumer instance per message', async () => {
    const driver = createControllableDriver();
    const seenIds = new Set<number>();
    let counter = 0;

    @Injectable({ scope: Scope.REQUEST })
    @KafkaConsumer('scoped')
    class ScopedConsumer {
      private readonly id = (counter += 1);

      @KafkaHandler()
      handle(): void {
        seenIds.add(this.id);
      }
    }

    const moduleRef = await Test.createTestingModule({
      imports: [
        KafkaModule.forRoot({ driverFactory: driver.factory }),
        KafkaModule.forFeature([ScopedConsumer]),
      ],
    }).compile();
    const app = await moduleRef.init();

    await driver.emit(messagePayload('scoped', { id: 1 }));
    await driver.emit(messagePayload('scoped', { id: 2 }));

    // Two messages → two distinct request-scoped instances.
    assert.equal(seenIds.size, 2);

    await app.close();
  });

  it('ignores methods without @KafkaHandler and a topic with no message', async () => {
    const driver = createControllableDriver();
    const received: number[] = [];

    @KafkaConsumer('mixed')
    class MixedConsumer {
      // A plain method on a consumer class must be ignored by the explorer.
      helper(): number {
        return 1;
      }

      @KafkaHandler()
      handle(payload: { n: number }): void {
        received.push(payload.n);
      }
    }

    const moduleRef = await Test.createTestingModule({
      imports: [
        KafkaModule.forRoot({ driverFactory: driver.factory }),
        KafkaModule.forFeature([MixedConsumer]),
      ],
    }).compile();
    const app = await moduleRef.init();

    // Only one topic subscribed even though the class has two methods.
    assert.deepEqual(driver.consumers[0].subscriptions, [{ topics: ['mixed'] }]);

    await driver.emit(messagePayload('mixed', { n: 7 }));
    assert.deepEqual(received, [7]);

    await app.close();
  });

  it('routes a shared topic to every handler registered for it', async () => {
    const driver = createControllableDriver();
    const hits: string[] = [];

    @KafkaConsumer(undefined, { groupId: 'shared' })
    class SharedConsumer {
      @KafkaHandler('shared-topic')
      first(): void {
        hits.push('first');
      }

      @KafkaHandler('shared-topic')
      second(): void {
        hits.push('second');
      }
    }

    const moduleRef = await Test.createTestingModule({
      imports: [
        KafkaModule.forRoot({ driverFactory: driver.factory }),
        KafkaModule.forFeature([SharedConsumer]),
      ],
    }).compile();
    const app = await moduleRef.init();

    assert.equal(driver.consumers.length, 1);
    assert.deepEqual(driver.consumers[0].subscriptions, [
      { topics: ['shared-topic'] },
    ]);

    await driver.emit(messagePayload('shared-topic', { id: 1 }));
    assert.deepEqual(hits.sort(), ['first', 'second']);

    await app.close();
  });

  it('ignores delivered messages for an unrouted topic', async () => {
    const driver = createControllableDriver();
    let calls = 0;

    @KafkaConsumer('known')
    class KnownConsumer {
      @KafkaHandler()
      handle(): void {
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

    // The broker delivers a record for a topic this consumer does not route.
    await driver.emit(messagePayload('not-routed', { id: 1 }));
    assert.equal(calls, 0);

    await driver.emit(messagePayload('known', { id: 1 }));
    assert.equal(calls, 1);

    await app.close();
  });

  it('drains an in-flight message before disconnecting on shutdown', async () => {
    const driver = createControllableDriver();
    let release: () => void = () => {};
    const gate = new Promise<void>(resolve => {
      release = resolve;
    });
    let completed = false;

    @KafkaConsumer('slow')
    class SlowConsumer {
      @KafkaHandler()
      async handle(): Promise<void> {
        await gate;
        completed = true;
      }
    }

    const moduleRef = await Test.createTestingModule({
      imports: [
        KafkaModule.forRoot({ driverFactory: driver.factory }),
        KafkaModule.forFeature([SlowConsumer]),
      ],
    }).compile();
    const app = await moduleRef.init();

    // Deliver without awaiting so the handler is parked on the gate, in flight.
    const eachMessage = driver.consumers[0].eachMessage;
    assert.ok(eachMessage);
    const inFlight = eachMessage(messagePayload('slow', { id: 1 }));

    // Begin shutdown while the handler is still parked, then let it finish.
    const closing = app.close();
    release();
    await closing;
    await inFlight;

    // The handler ran to completion before the consumer disconnected.
    assert.equal(completed, true);
    assert.equal(driver.consumers[0].disconnected, 1);
  });

  it('stops accepting new claims once shutdown has begun', async () => {
    const driver = createControllableDriver();
    const handled: number[] = [];

    @KafkaConsumer('gated')
    class GatedConsumer {
      @KafkaHandler()
      handle(payload: { id: number }): void {
        handled.push(payload.id);
      }
    }

    const moduleRef = await Test.createTestingModule({
      imports: [
        KafkaModule.forRoot({ driverFactory: driver.factory }),
        KafkaModule.forFeature([GatedConsumer]),
      ],
    }).compile();
    const app = await moduleRef.init();
    const eachMessage = driver.consumers[0].eachMessage;
    assert.ok(eachMessage);

    await eachMessage(messagePayload('gated', { id: 1 }));
    await app.close();

    // A record delivered after shutdown began is handed back — rejected, not
    // ignored: returning normally would tell the client it was processed, and
    // the client would commit it. The handler never runs for it.
    await assert.rejects(
      eachMessage(messagePayload('gated', { id: 2 })),
      /shutting down; the record from gated\[0\] was not handled/,
    );

    assert.deepEqual(handled, [1]);
  });

  it('pauses every consumer before draining it on shutdown', async () => {
    const driver = createControllableDriver();
    const events: string[] = [];
    let release: () => void = () => {};
    const gate = new Promise<void>(resolve => {
      release = resolve;
    });

    @KafkaConsumer('orders', { groupId: 'pause-orders' })
    class OrdersConsumer {
      @KafkaHandler()
      async handle(): Promise<void> {
        await gate;
        events.push('handled');
      }

      @KafkaHandler('refunds')
      refund(): void {}
    }

    const moduleRef = await Test.createTestingModule({
      imports: [
        KafkaModule.forRoot({ driverFactory: driver.factory }),
        KafkaModule.forFeature([OrdersConsumer]),
      ],
    }).compile();
    const app = await moduleRef.init();
    const [record] = driver.consumers;
    record.consumer.pause = topics => {
      events.push(`paused ${topics.map(({ topic }) => topic).join(',')}`);
      // The in-flight handler may finish only once the consumer is paused.
      release();
    };
    const disconnect = record.consumer.disconnect;
    record.consumer.disconnect = async () => {
      events.push('disconnected');
      await disconnect();
    };

    const inFlight = record.eachMessage?.(messagePayload('orders', { id: 1 }));
    // Failsafe: without a pause the handler would wait forever; release it
    // late instead, so a missing pause fails the assertion below, not a hang.
    const failsafe = setTimeout(release, 1_000);
    await app.close();
    await inFlight;
    clearTimeout(failsafe);

    // Paused first (so the client stops handing records over), then the
    // in-flight handler finished, then the consumer left the group.
    assert.deepEqual(events, ['paused orders,refunds', 'handled', 'disconnected']);
  });

  it('still drains and disconnects when pausing fails', async () => {
    // The client throws an Error; a custom driver may throw anything.
    for (const failure of [
      new Error('Pause can only be called while connected.'),
      'pause refused',
    ]) {
      const driver = createControllableDriver();

      @KafkaConsumer('fragile')
      class FragileConsumer {
        @KafkaHandler()
        handle(): void {}
      }

      const moduleRef = await Test.createTestingModule({
        imports: [
          KafkaModule.forRoot({ driverFactory: driver.factory }),
          KafkaModule.forFeature([FragileConsumer]),
        ],
      }).compile();
      const app = await moduleRef.init();
      const [record] = driver.consumers;
      record.consumer.pause = () => {
        throw failure;
      };

      await app.close();

      assert.equal(record.disconnected, 1, 'shutdown carried on and disconnected');
      // A late record is still handed back, paused or not.
      await assert.rejects(
        record.eachMessage?.(messagePayload('fragile', { id: 1 })) ??
          Promise.resolve(),
        /shutting down/,
      );
    }
  });

  it('honours a custom error mapper that commits every failure', async () => {
    const driver = createControllableDriver();
    const mapped: { topic: string; message: string }[] = [];

    @KafkaConsumer('mapped')
    class MappedConsumer {
      @KafkaHandler()
      handle(): void {
        throw new Error('always transient');
      }
    }

    const moduleRef = await Test.createTestingModule({
      imports: [
        KafkaModule.forRoot({
          driverFactory: driver.factory,
          // A plain Error would normally retry (rethrow); the custom mapper
          // records it and commits instead, so the emit resolves.
          errorMapper: (error, context) => {
            mapped.push({
              topic: context.getTopic(),
              message: (error as Error).message,
            });
            return 'commit';
          },
        }),
        KafkaModule.forFeature([MappedConsumer]),
      ],
    }).compile();
    const app = await moduleRef.init();

    await driver.emit(messagePayload('mapped', { id: 1 }));

    assert.deepEqual(mapped, [
      { topic: 'mapped', message: 'always transient' },
    ]);

    await app.close();
  });
});

/** Poll a condition across event-loop turns until it holds (or time runs out). */
async function eventually(predicate: () => boolean, timeoutMs = 2_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() > deadline) {
      throw new Error('condition never held');
    }
    await new Promise(resolve => setTimeout(resolve, 5));
  }
}

describe('Kafka retry backoff', () => {
  @KafkaConsumer('flaky')
  class FlakyConsumer {
    static failures = 0;

    @KafkaHandler()
    handle(payload: { fail: boolean }): void {
      if (payload.fail) {
        FlakyConsumer.failures += 1;
        throw new Error('downstream unavailable');
      }
    }
  }

  async function start(
    driver: ControllableDriver,
    retryBackoff: KafkaModuleOptions['retryBackoff'],
    consumers: Type[] = [FlakyConsumer],
  ) {
    const moduleRef = await Test.createTestingModule({
      imports: [
        KafkaModule.forRoot({ driverFactory: driver.factory, retryBackoff }),
        KafkaModule.forFeature(consumers),
      ],
    }).compile();
    return moduleRef.init();
  }

  it('pauses the partition of a retried record, then resumes it after the delay', async () => {
    const driver = createControllableDriver({ pausable: true });
    const app = await start(driver, { initialDelayMs: 20, maxDelayMs: 20 });
    const [record] = driver.consumers;

    await assert.rejects(
      record.eachMessage?.(messagePayload('flaky', { fail: true }, 2)) ??
        Promise.resolve(),
      /downstream unavailable/,
    );
    // Paused before the rejection reached the client, so its seek-back lands
    // on a partition that stays quiet for the delay.
    assert.deepEqual(record.pauses, ['pause flaky[2]']);
    await eventually(() => record.pauses.length === 2);
    assert.deepEqual(record.pauses, ['pause flaky[2]', 'resume flaky[2]']);

    // A success on the partition ends its streak; nothing else is paused.
    await record.eachMessage?.(messagePayload('flaky', { fail: false }, 2));
    assert.deepEqual(record.pauses, ['pause flaky[2]', 'resume flaky[2]']);

    await app.close();
  });

  it('redelivers immediately when retryBackoff is false', async () => {
    const driver = createControllableDriver({ pausable: true });
    const app = await start(driver, false);
    const [record] = driver.consumers;

    await assert.rejects(
      record.eachMessage?.(messagePayload('flaky', { fail: true })) ??
        Promise.resolve(),
      /downstream unavailable/,
    );
    assert.deepEqual(record.pauses, [], 'no backoff pause');

    await app.close();
    // Only the shutdown pause, which covers every assigned partition.
    assert.deepEqual(record.pauses, ['pause flaky[*]']);
  });

  it('backs off a failed batch on its partition', async () => {
    const driver = createControllableDriver({ pausable: true });

    @KafkaConsumer('metrics')
    class FailingBatchConsumer {
      @KafkaHandler(undefined, { batch: true })
      handle(): void {
        throw new Error('warehouse unavailable');
      }
    }

    const app = await start(
      driver,
      { initialDelayMs: 60_000, maxDelayMs: 60_000 },
      [FailingBatchConsumer],
    );
    const [record] = driver.consumers;

    for (const messages of [
      [{ value: '1', offset: '12' }],
      [], // a driver may hand over an empty batch; it still has a partition
    ]) {
      await assert.rejects(
        record.eachBatch?.({
          batch: { topic: 'metrics', partition: 4, messages },
          resolveOffset: () => {},
        }) ?? Promise.resolve(),
        /warehouse unavailable/,
      );
    }
    assert.deepEqual(record.pauses, ['pause metrics[4]', 'pause metrics[4]']);

    await app.close();
  });

  it('cancels a pending resume when the application shuts down', async () => {
    const driver = createControllableDriver({ pausable: true });
    const app = await start(driver, { initialDelayMs: 60_000, maxDelayMs: 60_000 });
    const [record] = driver.consumers;

    await assert.rejects(
      record.eachMessage?.(messagePayload('flaky', { fail: true })) ??
        Promise.resolve(),
    );
    await app.close();
    await new Promise(resolve => setTimeout(resolve, 30));

    // Paused for the backoff, paused again for shutdown — never resumed after
    // the consumer disconnected.
    assert.deepEqual(record.pauses, ['pause flaky[0]', 'pause flaky[*]']);
  });

  it('refuses an invalid retryBackoff at bootstrap', async () => {
    const driver = createControllableDriver({ pausable: true });
    await assert.rejects(start(driver, { multiplier: 0.5 }), /Invalid retryBackoff/);
  });
});

describe('Kafka async error mapper', () => {
  @KafkaConsumer('dead-letter-source')
  class PoisonConsumer {
    @KafkaHandler()
    handle(): void {
      throw new Error('poison record');
    }
  }

  async function start(driver: ControllableDriver, errorMapper: KafkaErrorMapper) {
    const moduleRef = await Test.createTestingModule({
      imports: [
        KafkaModule.forRoot({ driverFactory: driver.factory, errorMapper }),
        KafkaModule.forFeature([PoisonConsumer]),
      ],
    }).compile();
    return moduleRef.init();
  }

  it('awaits an async mapper that decides to retry', async () => {
    const driver = createControllableDriver();
    const app = await start(driver, async (): Promise<KafkaErrorBehavior> => {
      await new Promise(resolve => setTimeout(resolve, 5));
      return 'retry';
    });

    // 'retry' must reach the client as a rejection, or the record is committed.
    await assert.rejects(
      driver.consumers[0].eachMessage?.(messagePayload('dead-letter-source', { id: 1 })) ??
        Promise.resolve(),
      /poison record/,
    );
    await app.close();
  });

  it('commits only after an async mapper finished its dead-letter produce', async () => {
    const driver = createControllableDriver();
    const deadLettered: string[] = [];
    const app = await start(driver, async (error): Promise<KafkaErrorBehavior> => {
      await new Promise(resolve => setTimeout(resolve, 5));
      deadLettered.push(String(error));
      return 'commit';
    });

    await driver.consumers[0].eachMessage?.(messagePayload('dead-letter-source', { id: 1 }));
    // Resolving tells the client the record is done; the produce came first.
    assert.deepEqual(deadLettered, ['Error: poison record']);
    await app.close();
  });

  it('retries the record when an async mapper fails', async () => {
    const driver = createControllableDriver();
    const app = await start(driver, async (): Promise<KafkaErrorBehavior> => {
      throw new Error('dead-letter topic unavailable');
    });

    // The dead-letter produce failed, so the record must not be committed.
    await assert.rejects(
      driver.consumers[0].eachMessage?.(messagePayload('dead-letter-source', { id: 1 })) ??
        Promise.resolve(),
      /dead-letter topic unavailable/,
    );
    await app.close();
  });
});

describe('Kafka topic patterns', () => {
  async function start(driver: ControllableDriver, consumers: Type[]) {
    const moduleRef = await Test.createTestingModule({
      imports: [
        KafkaModule.forRoot({ driverFactory: driver.factory }),
        KafkaModule.forFeature(consumers),
      ],
    }).compile();
    return moduleRef.init();
  }

  it('subscribes by pattern and routes every topic it matches', async () => {
    const driver = createControllableDriver();
    const seen: string[] = [];

    @KafkaConsumer(/^orders\./, { groupId: 'order-events' })
    class OrderEvents {
      @KafkaHandler()
      handle(_payload: unknown, context: KafkaContext): void {
        seen.push(context.getTopic());
      }
    }

    const app = await start(driver, [OrderEvents]);
    const [record] = driver.consumers;
    assert.deepEqual(record.subscriptions, [{ topics: [/^orders\./] }]);

    for (const topic of ['orders.placed', 'orders.cancelled', 'payments.captured']) {
      await driver.emit(messagePayload(topic, { id: 1 }));
    }
    assert.deepEqual(seen, ['orders.placed', 'orders.cancelled']);

    await app.close();
  });

  it('runs the exact handlers, then the pattern handlers, for a topic both match', async () => {
    const driver = createControllableDriver();
    const calls: string[] = [];

    @KafkaConsumer(undefined, { groupId: 'orders' })
    class Orders {
      @KafkaHandler(/^orders\./)
      audit(): void {
        calls.push('audit');
      }

      @KafkaHandler('orders.placed')
      place(): void {
        calls.push('place');
      }

      @KafkaHandler(/^orders\./)
      metrics(): void {
        calls.push('metrics');
      }
    }

    const app = await start(driver, [Orders]);
    // One subscription per exact topic, one per distinct pattern.
    assert.deepEqual(driver.consumers[0].subscriptions, [
      { topics: ['orders.placed', /^orders\./] },
    ]);

    await driver.emit(messagePayload('orders.placed', { id: 1 }));
    assert.deepEqual(calls, ['place', 'audit', 'metrics']);

    calls.length = 0;
    await driver.emit(messagePayload('orders.cancelled', { id: 1 }));
    assert.deepEqual(calls, ['audit', 'metrics']);

    await app.close();
  });

  it('routes batches by pattern too', async () => {
    const driver = createControllableDriver();
    const sizes: number[] = [];

    @KafkaConsumer(/^metrics\./)
    class MetricsBatches {
      @KafkaHandler(undefined, { batch: true })
      aggregate(payloads: unknown[]): void {
        sizes.push(payloads.length);
      }
    }

    const app = await start(driver, [MetricsBatches]);
    await driver.consumers[0].eachBatch?.({
      batch: {
        topic: 'metrics.cpu',
        partition: 0,
        messages: [{ value: '1', offset: '0' }, { value: '2', offset: '1' }],
      },
      resolveOffset: () => {},
    });
    assert.deepEqual(sizes, [2]);

    await app.close();
  });

  it('pauses the topics a pattern delivered, as well as the named ones, on shutdown', async () => {
    const driver = createControllableDriver({ pausable: true });

    @KafkaConsumer(undefined, { groupId: 'mixed' })
    class Mixed {
      @KafkaHandler('named')
      named(): void {}

      @KafkaHandler(/^orders\./)
      orders(): void {}
    }

    const app = await start(driver, [Mixed]);
    await driver.emit(messagePayload('orders.placed', { id: 1 }));
    await driver.emit(messagePayload('orders.placed', { id: 2 }));
    await app.close();

    assert.deepEqual(driver.consumers[0].pauses, ['pause named[*],orders.placed[*]']);
  });

  for (const [label, pattern, message] of [
    ['an unanchored pattern', /orders\./, /must start with "\^" and carry no flags/],
    ['a pattern with flags', /^orders\./i, /must start with "\^" and carry no flags/],
    ['a backslash class', /^orders\.\d+/, /syntax only JavaScript understands/],
    ['a non-capturing group', /^(?:orders)/, /syntax only JavaScript understands/],
  ] as const) {
    it(`refuses ${label} at bootstrap`, async () => {
      @KafkaConsumer(pattern)
      class Invalid {
        @KafkaHandler()
        handle(): void {}
      }

      await assert.rejects(start(createControllableDriver(), [Invalid]), message);
    });
  }

  it('refuses a replying handler on a pattern', async () => {
    @KafkaConsumer(/^requests\./)
    class Replier {
      @KafkaHandler(undefined, { reply: true })
      answer(): string {
        return 'ok';
      }
    }

    await assert.rejects(
      start(createControllableDriver(), [Replier]),
      /"reply: true" on the pattern \/\^requests\\\.\/.*must name its request topic/s,
    );
  });
});
