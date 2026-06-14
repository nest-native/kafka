import assert from 'node:assert/strict';
import Module from 'node:module';
import { describe, it } from 'node:test';
import { Injectable } from '@nestjs/common';
import { Test } from '@nestjs/testing';
import {
  KAFKA_CLIENT_DRIVER,
  KAFKA_MODULE_OPTIONS,
  KAFKA_PRODUCER,
  KafkaModule,
} from '../kafka.module';
import { KafkaModuleOptions } from '../interfaces';
import {
  KafkaClientConfig,
  KafkaClientDriver,
  KafkaDriverFactory,
  KafkaDriverProducer,
  KafkaProducerConfig,
} from '../driver';
import { KafkaProducerService } from '../kafka-producer.service';

@Injectable()
class MarkerProvider {
  readonly name = 'marker';
}

@Injectable()
class OrdersHandler {
  readonly topic = 'orders';
}

interface FactoryInvocation {
  clientConfig: KafkaClientConfig;
  producerConfig: KafkaProducerConfig;
}

function createFakeProducer(): KafkaDriverProducer {
  return {
    connect: async () => {},
    disconnect: async () => {},
    send: async () => [],
    sendBatch: async () => [],
    transaction: async () => ({
      send: async () => [],
      sendBatch: async () => [],
      commit: async () => {},
      abort: async () => {},
    }),
  };
}

function createRecordingFactory(): {
  factory: KafkaDriverFactory;
  invocations: FactoryInvocation[];
  producers: KafkaDriverProducer[];
} {
  const invocations: FactoryInvocation[] = [];
  const producers: KafkaDriverProducer[] = [];

  const factory: KafkaDriverFactory = (clientConfig, producerConfig) => {
    invocations.push({ clientConfig, producerConfig });
    const driver: KafkaClientDriver = {
      createProducer: () => {
        const producer = createFakeProducer();
        producers.push(producer);
        return producer;
      },
    };
    return driver;
  };

  return { factory, invocations, producers };
}

/**
 * A driver factory that yields fake producers, used by option-focused tests
 * that compile the module but do not care about the driver wiring itself. It
 * keeps those tests from reaching for the optional Confluent peer.
 */
const noopFactory: KafkaDriverFactory = () => ({
  createProducer: createFakeProducer,
});

describe('KafkaModule', () => {
  it('provides default options when forRoot is called without arguments', async () => {
    const module = await Test.createTestingModule({
      imports: [KafkaModule.forRoot({ driverFactory: noopFactory })],
    }).compile();

    assert.deepEqual(module.get<KafkaModuleOptions>(KAFKA_MODULE_OPTIONS), {
      driverFactory: noopFactory,
    });
  });

  it('provides the supplied options through forRoot', async () => {
    const options: KafkaModuleOptions = {
      clientId: 'orders-service',
      driverFactory: noopFactory,
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
      driverFactory: noopFactory,
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
            driverFactory: noopFactory,
          }),
        }),
      ],
    }).compile();

    assert.deepEqual(module.get<KafkaModuleOptions>(KAFKA_MODULE_OPTIONS), {
      clientId: 'marker',
      driverFactory: noopFactory,
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

  it('wires the driver, raw producer, and producer service through forRoot', async () => {
    const { factory, invocations, producers } = createRecordingFactory();

    const module = await Test.createTestingModule({
      imports: [
        KafkaModule.forRoot({
          clientId: 'orders-service',
          client: { brokers: ['localhost:9092'] },
          producer: { allowAutoTopicCreation: false },
          driverFactory: factory,
        }),
      ],
    }).compile();
    await module.init();

    assert.deepEqual(invocations, [
      {
        clientConfig: {
          brokers: ['localhost:9092'],
          clientId: 'orders-service',
        },
        producerConfig: { allowAutoTopicCreation: false },
      },
    ]);

    const driver = module.get<KafkaClientDriver>(KAFKA_CLIENT_DRIVER);
    assert.equal(typeof driver.createProducer, 'function');
    assert.equal(module.get<KafkaDriverProducer>(KAFKA_PRODUCER), producers[0]);

    const producerService = module.get(KafkaProducerService);
    assert.ok(producerService instanceof KafkaProducerService);
    assert.equal(producerService.isConnected(), true);

    await module.close();
  });

  it('defaults the brokers list and the producer config when omitted', async () => {
    const { factory, invocations } = createRecordingFactory();

    const module = await Test.createTestingModule({
      imports: [KafkaModule.forRoot({ driverFactory: factory })],
    }).compile();

    module.get<KafkaDriverProducer>(KAFKA_PRODUCER);

    assert.deepEqual(invocations, [
      { clientConfig: { brokers: [] }, producerConfig: {} },
    ]);
  });

  it('keeps the client brokers when no clientId convenience option is given', async () => {
    const { factory, invocations } = createRecordingFactory();

    const module = await Test.createTestingModule({
      imports: [
        KafkaModule.forRoot({
          client: { brokers: ['broker:9092'], clientId: 'from-client' },
          driverFactory: factory,
        }),
      ],
    }).compile();

    module.get<KafkaDriverProducer>(KAFKA_PRODUCER);

    assert.deepEqual(invocations, [
      {
        clientConfig: { brokers: ['broker:9092'], clientId: 'from-client' },
        producerConfig: {},
      },
    ]);
  });

  it('wires the producer service through forRootAsync', async () => {
    const { factory, invocations } = createRecordingFactory();

    const module = await Test.createTestingModule({
      imports: [
        KafkaModule.forRootAsync({
          inject: [MarkerProvider],
          extraProviders: [MarkerProvider],
          useFactory: (marker: MarkerProvider) => ({
            clientId: marker.name,
            client: { brokers: ['async-broker:9092'] },
            driverFactory: factory,
          }),
        }),
      ],
    }).compile();
    await module.init();

    assert.deepEqual(invocations, [
      {
        clientConfig: { brokers: ['async-broker:9092'], clientId: 'marker' },
        producerConfig: {},
      },
    ]);
    assert.ok(module.get(KafkaProducerService) instanceof KafkaProducerService);

    await module.close();
  });

  it('falls back to the default Confluent driver when no factory is supplied', async () => {
    const constructorConfigs: unknown[] = [];
    const fakeProducer = createFakeProducer();

    class FakeKafka {
      constructor(config?: unknown) {
        constructorConfigs.push(config);
      }

      producer(): KafkaDriverProducer {
        return fakeProducer;
      }
    }

    const moduleWithLoad = Module as unknown as {
      _load: (request: string, parent: unknown, isMain: boolean) => unknown;
    };
    const realLoad = moduleWithLoad._load;
    moduleWithLoad._load = (request, parent, isMain) =>
      request === '@confluentinc/kafka-javascript'
        ? { KafkaJS: { Kafka: FakeKafka } }
        : realLoad(request, parent, isMain);

    try {
      const module = await Test.createTestingModule({
        imports: [
          KafkaModule.forRoot({ client: { brokers: ['localhost:9092'] } }),
        ],
      }).compile();

      assert.equal(
        module.get<KafkaDriverProducer>(KAFKA_PRODUCER),
        fakeProducer,
      );
      assert.deepEqual(constructorConfigs, [
        { kafkaJS: { brokers: ['localhost:9092'] } },
      ]);
    } finally {
      moduleWithLoad._load = realLoad;
    }
  });
});
