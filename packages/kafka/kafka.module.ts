import { DynamicModule, Module, Provider, Type } from '@nestjs/common';
import { KafkaModuleAsyncOptions, KafkaModuleOptions } from './interfaces';

/**
 * Injection token for the resolved {@link KafkaModuleOptions}.
 *
 * Consumers that need the global Kafka configuration can inject this token. The
 * consumer/producer building blocks added in later milestones resolve their
 * defaults from the same provider.
 */
export const KAFKA_MODULE_OPTIONS = Symbol('KAFKA_MODULE_OPTIONS');

/**
 * Root module for `@nest-native/kafka`.
 *
 * At this scaffold milestone the module only registers global configuration and
 * a feature-module entry point so applications can wire it into their root and
 * feature modules. The `@KafkaConsumer`, `@KafkaHandler`, parameter decorators,
 * and `KafkaProducerService` arrive in later milestones and build on this same
 * module shell.
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
      providers: [optionsProvider],
      exports: [optionsProvider],
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
      imports: options.imports ?? [],
      providers: [...(options.extraProviders ?? []), optionsProvider],
      exports: [optionsProvider],
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
}
