import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { ApplicationConfig, MetadataScanner } from '@nestjs/core';
import { KAFKA_CONSUMER_METADATA } from '../constants';
import { KafkaConsumerExplorer } from '../kafka-consumer.explorer';
import { KafkaConsumer } from '../kafka-consumer.decorator';
import { KafkaHandler } from '../kafka-handler.decorator';
import {
  KafkaClientDriver,
  KafkaConsumerConfig,
  KafkaDriverConsumer,
  KafkaEachMessageHandler,
} from '../driver';

/**
 * These tests drive the explorer with hand-built collaborators instead of a full
 * Nest application. That makes the defensive fallbacks reachable: a provider
 * wrapper without `isDependencyTreeStatic` and a handler method without emitted
 * `design:paramtypes` metadata — neither of which a normal Nest bootstrap
 * produces, but both of which the explorer must tolerate.
 */

function fakeReflector() {
  return {
    get: (_key: unknown, target: unknown) =>
      Reflect.getMetadata(KAFKA_CONSUMER_METADATA, target as object),
  };
}

function recordingDriver(): {
  driver: KafkaClientDriver;
  configs: KafkaConsumerConfig[];
  trigger: (topic: string, value: string) => Promise<void>;
} {
  const configs: KafkaConsumerConfig[] = [];
  let each: KafkaEachMessageHandler | undefined;

  const consumer: KafkaDriverConsumer = {
    connect: async () => {},
    disconnect: async () => {},
    subscribe: async () => {},
    run: async config => {
      each = config.eachMessage;
    },
  };

  const driver: KafkaClientDriver = {
    createProducer: () => {
      throw new Error('not used');
    },
    createConsumer: config => {
      configs.push(config ?? {});
      return consumer;
    },
  };

  const trigger = async (topic: string, value: string): Promise<void> => {
    await each?.({ topic, partition: 0, message: { value } });
  };

  return { driver, configs, trigger };
}

function buildExplorer(
  instance: object,
  driver: KafkaClientDriver,
  wrapperExtras: Record<string, unknown> = {},
): KafkaConsumerExplorer {
  const wrapper = { instance, metatype: instance.constructor, ...wrapperExtras };
  const moduleRef = { providers: new Map([[instance.constructor, wrapper]]) };
  const modulesContainer = new Map([['module-key', moduleRef]]);

  return new KafkaConsumerExplorer(
    new MetadataScanner(),
    modulesContainer as never,
    fakeReflector() as never,
    new ApplicationConfig(),
    { resolve: async () => instance } as never,
    driver,
  );
}

describe('KafkaConsumerExplorer (unit)', () => {
  it('defaults paramTypes and treats a wrapper without scope info as static', async () => {
    const received: unknown[] = [];

    @KafkaConsumer('plain')
    class PlainConsumer {
      @KafkaHandler()
      handle(payload: unknown): void {
        received.push(payload);
      }
    }

    const instance = new PlainConsumer();
    // Strip the emitted param-type metadata to force the `?? []` fallback, and
    // omit `isDependencyTreeStatic` to force the `?? true` (static) fallback.
    Reflect.deleteMetadata(
      'design:paramtypes',
      Object.getPrototypeOf(instance),
      'handle',
    );

    const { driver, trigger } = recordingDriver();
    const explorer = buildExplorer(instance, driver);

    await explorer.onApplicationBootstrap();
    await trigger('plain', JSON.stringify({ ok: true }));

    assert.deepEqual(received, [{ ok: true }]);

    await explorer.onApplicationShutdown();
  });

  it('skips providers that have no instance or metatype', async () => {
    const moduleRef = {
      providers: new Map<unknown, unknown>([
        ['empty', { instance: undefined, metatype: undefined }],
      ]),
    };
    const modulesContainer = new Map([['m', moduleRef]]);
    const { driver, configs } = recordingDriver();

    const explorer = new KafkaConsumerExplorer(
      new MetadataScanner(),
      modulesContainer as never,
      fakeReflector() as never,
      new ApplicationConfig(),
      { resolve: async () => ({}) } as never,
      driver,
    );

    await explorer.onApplicationBootstrap();

    assert.equal(configs.length, 0);
  });
});
