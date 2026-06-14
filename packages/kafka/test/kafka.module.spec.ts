import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { Injectable } from '@nestjs/common';
import { Test } from '@nestjs/testing';
import { KAFKA_MODULE_OPTIONS, KafkaModule } from '../kafka.module';
import { KafkaModuleOptions } from '../interfaces';

@Injectable()
class MarkerProvider {
  readonly name = 'marker';
}

@Injectable()
class OrdersHandler {
  readonly topic = 'orders';
}

describe('KafkaModule', () => {
  it('provides default options when forRoot is called without arguments', async () => {
    const module = await Test.createTestingModule({
      imports: [KafkaModule.forRoot()],
    }).compile();

    assert.deepEqual(module.get<KafkaModuleOptions>(KAFKA_MODULE_OPTIONS), {});
  });

  it('provides the supplied options through forRoot', async () => {
    const options: KafkaModuleOptions = {
      clientId: 'orders-service',
    };

    const module = await Test.createTestingModule({
      imports: [KafkaModule.forRoot(options)],
    }).compile();

    assert.equal(module.get<KafkaModuleOptions>(KAFKA_MODULE_OPTIONS), options);
  });

  it('is global by default and allows explicit opt-out via forRoot', () => {
    assert.equal(KafkaModule.forRoot().global, true);
    assert.equal(KafkaModule.forRoot({ isGlobal: true }).global, true);
    assert.equal(KafkaModule.forRoot({ isGlobal: false }).global, false);
  });

  it('resolves options through forRootAsync useFactory', async () => {
    const options: KafkaModuleOptions = {
      clientId: 'async-service',
    };

    const module = await Test.createTestingModule({
      imports: [
        KafkaModule.forRootAsync({
          useFactory: async () => options,
        }),
      ],
    }).compile();

    assert.equal(module.get<KafkaModuleOptions>(KAFKA_MODULE_OPTIONS), options);
  });

  it('injects dependencies and registers extra providers in forRootAsync', async () => {
    const module = await Test.createTestingModule({
      imports: [
        KafkaModule.forRootAsync({
          imports: [],
          inject: [MarkerProvider],
          extraProviders: [MarkerProvider],
          useFactory: (marker: MarkerProvider) => ({
            clientId: marker.name,
          }),
        }),
      ],
    }).compile();

    assert.deepEqual(module.get<KafkaModuleOptions>(KAFKA_MODULE_OPTIONS), {
      clientId: 'marker',
    });
    assert.equal(module.get(MarkerProvider).name, 'marker');
  });

  it('is global by default and allows explicit opt-out via forRootAsync', () => {
    const defaulted = KafkaModule.forRootAsync({
      useFactory: () => ({}),
    });
    const explicitTrue = KafkaModule.forRootAsync({
      isGlobal: true,
      useFactory: () => ({}),
    });
    const explicitFalse = KafkaModule.forRootAsync({
      isGlobal: false,
      useFactory: () => ({}),
    });

    assert.equal(defaulted.global, true);
    assert.deepEqual(defaulted.imports, []);
    assert.equal(explicitTrue.global, true);
    assert.equal(explicitFalse.global, false);
  });

  it('registers no providers when forFeature is called without handlers', () => {
    const dynamicModule = KafkaModule.forFeature();

    assert.equal(dynamicModule.module, KafkaModule);
    assert.equal(dynamicModule.global, undefined);
    assert.deepEqual(dynamicModule.providers, []);
    assert.deepEqual(dynamicModule.exports, []);
  });

  it('registers and exports the supplied handlers through forFeature', async () => {
    const dynamicModule = KafkaModule.forFeature([OrdersHandler]);

    assert.deepEqual(dynamicModule.providers, [OrdersHandler]);
    assert.deepEqual(dynamicModule.exports, [OrdersHandler]);

    const module = await Test.createTestingModule({
      imports: [KafkaModule.forFeature([OrdersHandler])],
    }).compile();

    assert.equal(module.get(OrdersHandler).topic, 'orders');
  });
});
