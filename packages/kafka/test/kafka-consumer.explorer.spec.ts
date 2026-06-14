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
  KafkaEachMessageHandler,
  KafkaEachMessagePayload,
  KafkaSubscription,
} from '../driver';
import { KafkaConsumer } from '../kafka-consumer.decorator';
import { KafkaHandler } from '../kafka-handler.decorator';
import { KafkaContext } from '../kafka-context';
import { KafkaModule } from '../kafka.module';

/** Silence Nest's bootstrap logging during the tests. */
Logger.overrideLogger(false);

interface RecordedConsumer {
  config: KafkaConsumerConfig;
  consumer: KafkaDriverConsumer;
  subscriptions: KafkaSubscription[];
  eachMessage?: KafkaEachMessageHandler;
  connected: number;
  disconnected: number;
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

function createControllableDriver(): ControllableDriver {
  const consumers: RecordedConsumer[] = [];

  const driver: KafkaClientDriver = {
    createProducer: noopProducer,
    createConsumer: (config = {}) => {
      const record: RecordedConsumer = {
        config,
        subscriptions: [],
        connected: 0,
        disconnected: 0,
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
        },
      };
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

    // A record delivered after shutdown began is refused (offset uncommitted),
    // so the handler never runs for it.
    await eachMessage(messagePayload('gated', { id: 2 }));

    assert.deepEqual(handled, [1]);
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
