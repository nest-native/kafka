import { ModuleMetadata, Provider } from '@nestjs/common';
import {
  KafkaClientConfig,
  KafkaDriverFactory,
  KafkaProducerConfig,
} from './driver';

/**
 * Configuration for {@link KafkaModule.forRoot}.
 *
 * This milestone wires the global configuration plus the producer service. The
 * consumer decorators (`@KafkaConsumer`, `@KafkaHandler`), the parameter
 * decorators, and batch/transactional consumption land in later milestones and
 * read from these same options.
 */
export interface KafkaModuleOptions {
  /**
   * Whether to register this module globally so the configuration and producer
   * service are available to every feature module without re-importing.
   *
   * @default true
   */
  isGlobal?: boolean;

  /**
   * Default client identifier reported to the broker for connections opened by
   * the producer (and the consumers added in later milestones).
   *
   * When set it is merged into {@link KafkaModuleOptions.client} as the
   * `clientId`, so the convenience option and the full connection config stay
   * in sync.
   */
  clientId?: string;

  /**
   * Connection configuration forwarded to the Confluent client. `brokers` is
   * required to open a real connection; omit the whole object only when you
   * supply a custom {@link KafkaModuleOptions.driverFactory} (for example in
   * unit tests that never reach a broker).
   */
  client?: KafkaClientConfig;

  /**
   * Producer configuration forwarded to the Confluent producer constructor.
   */
  producer?: KafkaProducerConfig;

  /**
   * Advanced override for the driver factory. Defaults to the lazily-resolved
   * Confluent driver. Supply a fake driver here to unit-test producers and
   * handlers without a broker.
   */
  driverFactory?: KafkaDriverFactory;
}

/**
 * Configuration for {@link KafkaModule.forRootAsync}.
 */
export interface KafkaModuleAsyncOptions
  extends Pick<ModuleMetadata, 'imports'> {
  /**
   * Whether to register this module globally.
   *
   * @default true
   */
  isGlobal?: boolean;

  /**
   * Providers to inject into {@link KafkaModuleAsyncOptions.useFactory}.
   */
  inject?: any[];

  /**
   * Additional providers registered alongside the resolved options.
   */
  extraProviders?: Provider[];

  /**
   * Factory that resolves the {@link KafkaModuleOptions} asynchronously.
   */
  useFactory: (
    ...args: any[]
  ) => KafkaModuleOptions | Promise<KafkaModuleOptions>;
}
