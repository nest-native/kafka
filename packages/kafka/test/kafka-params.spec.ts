import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import {
  Injectable,
  Logger,
  PipeTransform,
  UsePipes,
} from '@nestjs/common';
import { Test } from '@nestjs/testing';
import {
  KafkaClientDriver,
  KafkaDriverConsumer,
  KafkaDriverFactory,
  KafkaDriverProducer,
  KafkaEachMessageHandler,
  KafkaEachMessagePayload,
  KafkaMessageHeaders,
} from '../driver';
import { KafkaConsumer } from '../kafka-consumer.decorator';
import { KafkaHandler } from '../kafka-handler.decorator';
import { KafkaContext } from '../kafka-context';
import {
  KafkaCtx,
  KafkaHeaders,
  KafkaMessage,
} from '../kafka-params.decorators';
import { KafkaModule } from '../kafka.module';

Logger.overrideLogger(false);

interface Harness {
  factory: KafkaDriverFactory;
  emit: (payload: KafkaEachMessagePayload) => Promise<void>;
}

function harness(): Harness {
  let each: KafkaEachMessageHandler | undefined;
  const producer: KafkaDriverProducer = {
    connect: async () => {},
    disconnect: async () => {},
    send: async () => [],
    sendBatch: async () => [],
    transaction: async () => {
      throw new Error('unused');
    },
  };
  const consumer: KafkaDriverConsumer = {
    connect: async () => {},
    disconnect: async () => {},
    subscribe: async () => {},
    run: async config => {
      each = config.eachMessage;
    },
  };
  const driver: KafkaClientDriver = {
    createProducer: () => producer,
    createConsumer: () => consumer,
  };
  return {
    factory: () => driver,
    emit: payload => each?.(payload) ?? Promise.resolve(),
  };
}

function payload(
  topic: string,
  value: unknown,
  headers?: KafkaMessageHeaders,
): KafkaEachMessagePayload {
  return {
    topic,
    partition: 2,
    message: {
      value: typeof value === 'string' ? value : JSON.stringify(value),
      offset: '7',
      headers,
    },
  };
}

describe('Kafka parameter decorators', () => {
  it('injects the whole payload, headers map, and raw context', async () => {
    const h = harness();
    let captured:
      | { order: unknown; headers: KafkaMessageHeaders; topic: string }
      | undefined;

    @KafkaConsumer('orders')
    class OrdersConsumer {
      @KafkaHandler()
      handle(
        @KafkaMessage() order: unknown,
        @KafkaHeaders() headers: KafkaMessageHeaders,
        @KafkaCtx() context: KafkaContext,
      ): void {
        captured = { order, headers, topic: context.getTopic() };
      }
    }

    const moduleRef = await Test.createTestingModule({
      imports: [
        KafkaModule.forRoot({ driverFactory: h.factory }),
        KafkaModule.forFeature([OrdersConsumer]),
      ],
    }).compile();
    const app = await moduleRef.init();

    await h.emit(
      payload('orders', { id: 'o-1' }, { 'trace-id': 'abc' }),
    );

    assert.deepEqual(captured?.order, { id: 'o-1' });
    assert.deepEqual(captured?.headers, { 'trace-id': 'abc' });
    assert.equal(captured?.topic, 'orders');

    await app.close();
  });

  it('injects a payload property and a single header by key', async () => {
    const h = harness();
    let captured: { id: unknown; trace: unknown } | undefined;

    @KafkaConsumer('keyed')
    class KeyedConsumer {
      @KafkaHandler()
      handle(
        @KafkaMessage('id') id: unknown,
        @KafkaHeaders('trace-id') trace: unknown,
      ): void {
        captured = { id, trace };
      }
    }

    const moduleRef = await Test.createTestingModule({
      imports: [
        KafkaModule.forRoot({ driverFactory: h.factory }),
        KafkaModule.forFeature([KeyedConsumer]),
      ],
    }).compile();
    const app = await moduleRef.init();

    await h.emit(payload('keyed', { id: 42 }, { 'trace-id': 'xyz' }));

    assert.deepEqual(captured, { id: 42, trace: 'xyz' });

    await app.close();
  });

  it('resolves an empty header map and undefined property on a non-object payload', async () => {
    const h = harness();
    let captured:
      | { headers: KafkaMessageHeaders; missing: unknown; whole: unknown }
      | undefined;

    @KafkaConsumer('scalars')
    class ScalarsConsumer {
      @KafkaHandler()
      handle(
        @KafkaHeaders() headers: KafkaMessageHeaders,
        @KafkaMessage('id') missing: unknown,
        @KafkaMessage() whole: unknown,
      ): void {
        captured = { headers, missing, whole };
      }
    }

    const moduleRef = await Test.createTestingModule({
      imports: [
        KafkaModule.forRoot({ driverFactory: h.factory }),
        KafkaModule.forFeature([ScalarsConsumer]),
      ],
    }).compile();
    const app = await moduleRef.init();

    // A plain-string payload is not an object, so `@KafkaMessage('id')` is
    // undefined; the message carries no headers, so `@KafkaHeaders()` is empty.
    await h.emit(payload('scalars', 'plain-text'));

    assert.deepEqual(captured?.headers, {});
    assert.equal(captured?.missing, undefined);
    assert.equal(captured?.whole, 'plain-text');

    await app.close();
  });

  it('runs a param-level pipe alongside the decorator', async () => {
    const h = harness();
    let captured: unknown;

    @Injectable()
    class UppercasePipe implements PipeTransform {
      transform(value: unknown): unknown {
        return typeof value === 'string' ? value.toUpperCase() : value;
      }
    }

    @KafkaConsumer('piped')
    class PipedConsumer {
      @KafkaHandler()
      handle(@KafkaMessage('name', UppercasePipe) name: unknown): void {
        captured = name;
      }
    }

    const moduleRef = await Test.createTestingModule({
      imports: [KafkaModule.forRoot({ driverFactory: h.factory })],
      providers: [PipedConsumer, UppercasePipe],
    }).compile();
    const app = await moduleRef.init();

    await h.emit(payload('piped', { name: 'acme' }));

    assert.equal(captured, 'ACME');

    await app.close();
  });

  it('combines @UsePipes with parameter decorators', async () => {
    const h = harness();
    let captured: unknown;

    @Injectable()
    class WrapPipe implements PipeTransform {
      transform(value: unknown): unknown {
        return { wrapped: value };
      }
    }

    @KafkaConsumer('wrapped')
    class WrappedConsumer {
      @KafkaHandler()
      @UsePipes(WrapPipe)
      handle(@KafkaMessage() body: unknown): void {
        captured = body;
      }
    }

    const moduleRef = await Test.createTestingModule({
      imports: [KafkaModule.forRoot({ driverFactory: h.factory })],
      providers: [WrappedConsumer, WrapPipe],
    }).compile();
    const app = await moduleRef.init();

    await h.emit(payload('wrapped', { id: 1 }));

    // The method-level pipe runs over the resolved first (payload) argument.
    assert.deepEqual(captured, { wrapped: { id: 1 } });

    await app.close();
  });
});
