import { DynamicModule, Inject, Module, Provider } from '@nestjs/common';
import { KafkaModule } from '../kafka.module';
import { KafkaModuleAsyncOptions, KafkaModuleOptions } from '../interfaces';
import { KAFKA_TEST_BROKER } from '../tokens';
import { InMemoryKafkaBroker } from './in-memory-kafka-broker';

export { KAFKA_TEST_BROKER } from '../tokens';

/**
 * Options for {@link KafkaTestModule.forRoot}.
 *
 * They are the same as {@link KafkaModuleOptions} except the driver is fixed to
 * the in-memory broker, so `driverFactory` is not accepted (supplying your own
 * driver would defeat the purpose of the test module). Every other option —
 * `errorMapper`, `concurrency`, `maxInFlight`, … — passes straight through so the
 * code under test sees the same configuration it would in production.
 */
export type KafkaTestModuleOptions = Omit<KafkaModuleOptions, 'driverFactory'> & {
  /**
   * Reuse an existing {@link InMemoryKafkaBroker} instead of letting the module
   * create one. Useful when a test wants to hold the broker reference before the
   * module is compiled, or share one broker across several modules.
   */
  broker?: InMemoryKafkaBroker;
};

/**
 * Options for {@link KafkaTestModule.forRootAsync}. Identical to
 * {@link KafkaModuleAsyncOptions} except the resolved options may not carry a
 * `driverFactory` (the in-memory broker is always used).
 */
export interface KafkaTestModuleAsyncOptions
  extends Omit<KafkaModuleAsyncOptions, 'useFactory'> {
  /**
   * Reuse an existing {@link InMemoryKafkaBroker} instead of creating one.
   */
  broker?: InMemoryKafkaBroker;

  /**
   * Factory that resolves the {@link KafkaTestModuleOptions} asynchronously.
   */
  useFactory: (
    ...args: any[]
  ) => KafkaTestModuleOptions | Promise<KafkaTestModuleOptions>;
}

/**
 * Drop-in replacement for {@link KafkaModule} that runs the whole transport —
 * producer service, consumer decorators, the full Nest enhancer pipeline, batch
 * consumption, transactions, graceful shutdown — against an in-memory
 * {@link InMemoryKafkaBroker} instead of a real Kafka cluster.
 *
 * It is the "in-memory transport for unit tests" the public API promises: import
 * `KafkaTestModule.forRoot()` in place of `KafkaModule.forRoot()` in a
 * `Test.createTestingModule`, then inject the broker (with
 * `@InjectKafkaTestBroker()` or the `KAFKA_TEST_BROKER` token) to assert on the
 * messages handlers produced or to inject messages for them to consume — no
 * broker, no native `librdkafka`, no `KAFKA_BROKERS` env required.
 *
 * @example
 * ```ts
 * const moduleRef = await Test.createTestingModule({
 *   imports: [KafkaTestModule.forRoot(), OrdersModule],
 * }).compile();
 * const app = moduleRef.createNestApplication();
 * await app.init();
 *
 * const broker = app.get<InMemoryKafkaBroker>(KAFKA_TEST_BROKER);
 * await app.get(OrdersService).placeOrder({ id: '1' });
 * await broker.idle(); // every in-flight handler pipeline has settled
 * assert.equal(broker.getSentTo('orders.placed').length, 1);
 * ```
 *
 * @publicApi
 */
@Module({})
export class KafkaTestModule {
  /**
   * Register the test module with synchronous configuration, defaulting to an
   * empty configuration so `KafkaTestModule.forRoot()` just works.
   */
  static forRoot(options: KafkaTestModuleOptions = {}): DynamicModule {
    const { broker, moduleOptions } = this.split(options);
    return this.assemble(
      broker,
      KafkaModule.forRoot({
        ...moduleOptions,
        driverFactory: broker.createDriverFactory(),
      }),
      moduleOptions.isGlobal,
    );
  }

  /**
   * Register the test module with asynchronous configuration resolved through a
   * factory, mirroring {@link KafkaModule.forRootAsync} but pinned to the
   * in-memory broker.
   */
  static forRootAsync(options: KafkaTestModuleAsyncOptions): DynamicModule {
    const broker = options.broker ?? new InMemoryKafkaBroker();
    const driverFactory = broker.createDriverFactory();

    return this.assemble(
      broker,
      KafkaModule.forRootAsync({
        isGlobal: options.isGlobal,
        imports: options.imports,
        inject: options.inject,
        extraProviders: options.extraProviders,
        useFactory: async (...args: unknown[]) => {
          const resolved = await options.useFactory(...args);
          return { ...resolved, driverFactory };
        },
      }),
      options.isGlobal,
    );
  }

  /**
   * Split the public options into the broker (reused or freshly created) and the
   * {@link KafkaModuleOptions} forwarded to the underlying module.
   */
  private static split(options: KafkaTestModuleOptions): {
    broker: InMemoryKafkaBroker;
    moduleOptions: Omit<KafkaTestModuleOptions, 'broker'>;
  } {
    const { broker: provided, ...moduleOptions } = options;
    return {
      broker: provided ?? new InMemoryKafkaBroker(),
      moduleOptions,
    };
  }

  /**
   * Build the dynamic module that imports the configured {@link KafkaModule} and
   * additionally exposes the broker under {@link KAFKA_TEST_BROKER}.
   */
  private static assemble(
    broker: InMemoryKafkaBroker,
    kafkaModule: DynamicModule,
    isGlobal: boolean | undefined,
  ): DynamicModule {
    const brokerProvider: Provider = {
      provide: KAFKA_TEST_BROKER,
      useValue: broker,
    };

    return {
      module: KafkaTestModule,
      global: isGlobal ?? true,
      imports: [kafkaModule],
      providers: [brokerProvider],
      exports: [brokerProvider, kafkaModule],
    };
  }
}

/**
 * Inject the {@link InMemoryKafkaBroker} backing {@link KafkaTestModule}.
 *
 * @example
 * ```ts
 * class OrdersAssertions {
 *   constructor(@InjectKafkaTestBroker() private readonly broker: InMemoryKafkaBroker) {}
 * }
 * ```
 *
 * @publicApi
 */
export const InjectKafkaTestBroker = (): ParameterDecorator =>
  Inject(KAFKA_TEST_BROKER);
