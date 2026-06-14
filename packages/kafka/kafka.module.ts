import { DynamicModule, Module, Provider, Type } from '@nestjs/common';
import { DiscoveryModule } from '@nestjs/core';
import {
  KafkaClientConfig,
  KafkaClientDriver,
  KafkaDriverProducer,
  createConfluentDriver,
} from './driver';
import { KafkaProducerService } from './kafka-producer.service';
import { KafkaConsumerExplorer } from './kafka-consumer.explorer';
import { KafkaModuleAsyncOptions, KafkaModuleOptions } from './interfaces';
import {
  KAFKA_CLIENT_DRIVER,
  KAFKA_MODULE_OPTIONS,
  KAFKA_PRODUCER,
} from './tokens';

export {
  KAFKA_CLIENT_DRIVER,
  KAFKA_MODULE_OPTIONS,
  KAFKA_PRODUCER,
} from './tokens';

/**
 * Root module for `@nest-native/kafka`.
 *
 * This milestone wires the global configuration, the Kafka driver, a single
 * shared producer, and the {@link KafkaProducerService}. The `@KafkaConsumer`,
 * `@KafkaHandler`, parameter decorators, and batch/transactional consumption
 * arrive in later milestones and build on this same module shell.
 */
@Module({})
export class KafkaModule {
  /**
   * Register the module with synchronous configuration.
   */
  static forRoot(options: KafkaModuleOptions = {}): DynamicModule {
    const optionsProvider: Provider = {
      provide: KAFKA_MODULE_OPTIONS,
      useValue: options,
    };

    return {
      module: KafkaModule,
      global: options.isGlobal ?? true,
      imports: [DiscoveryModule],
      providers: [optionsProvider, ...this.coreProviders()],
      exports: this.exportedProviders(),
    };
  }

  /**
   * Register the module with asynchronous configuration resolved through a
   * factory.
   */
  static forRootAsync(options: KafkaModuleAsyncOptions): DynamicModule {
    const optionsProvider: Provider = {
      provide: KAFKA_MODULE_OPTIONS,
      useFactory: options.useFactory,
      inject: options.inject ?? [],
    };

    return {
      module: KafkaModule,
      global: options.isGlobal ?? true,
      imports: [DiscoveryModule, ...(options.imports ?? [])],
      providers: [
        ...(options.extraProviders ?? []),
        optionsProvider,
        ...this.coreProviders(),
      ],
      exports: this.exportedProviders(),
    };
  }

  /**
   * Register feature-scoped consumer handlers.
   *
   * Returns a non-global {@link DynamicModule} that registers the supplied
   * handler classes as providers so the consumer decorators added in later
   * milestones can resolve them through Nest dependency injection.
   */
  static forFeature(handlers: Type[] = []): DynamicModule {
    return {
      module: KafkaModule,
      providers: [...handlers],
      exports: [...handlers],
    };
  }

  /**
   * Providers shared by {@link forRoot} and {@link forRootAsync}: the driver,
   * the raw producer, and the producer service.
   */
  private static coreProviders(): Provider[] {
    return [
      {
        provide: KAFKA_CLIENT_DRIVER,
        useFactory: (options: KafkaModuleOptions): KafkaClientDriver =>
          this.createDriver(options),
        inject: [KAFKA_MODULE_OPTIONS],
      },
      {
        provide: KAFKA_PRODUCER,
        useFactory: (driver: KafkaClientDriver): KafkaDriverProducer =>
          driver.createProducer(),
        inject: [KAFKA_CLIENT_DRIVER],
      },
      KafkaProducerService,
      KafkaConsumerExplorer,
    ];
  }

  private static exportedProviders(): NonNullable<DynamicModule['exports']> {
    return [
      KAFKA_MODULE_OPTIONS,
      KAFKA_CLIENT_DRIVER,
      KAFKA_PRODUCER,
      KafkaProducerService,
    ];
  }

  /**
   * Build the driver from the resolved options, merging the `clientId`
   * convenience option into the client config and defaulting to the lazily
   * resolved Confluent driver.
   */
  private static createDriver(options: KafkaModuleOptions): KafkaClientDriver {
    const factory = options.driverFactory ?? createConfluentDriver;
    const clientConfig = this.resolveClientConfig(options);
    return factory(clientConfig, options.producer ?? {});
  }

  private static resolveClientConfig(
    options: KafkaModuleOptions,
  ): KafkaClientConfig {
    const clientConfig: KafkaClientConfig = {
      brokers: [],
      ...options.client,
    };

    if (options.clientId !== undefined) {
      clientConfig.clientId = options.clientId;
    }

    return clientConfig;
  }
}
