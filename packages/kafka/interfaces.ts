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
/**
 * Options accepted by {@link KafkaConsumer}.
 *
 * The class-level decorator may carry a consumer-group identifier shared by all
 * of its handler methods. Confluent groups consumers so partitions are balanced
 * across instances; leaving it unset lets each application choose its own group
 * through {@link KafkaModuleOptions} or the broker default.
 */
export interface KafkaConsumerOptions {
  /**
   * The Kafka consumer group this consumer joins. When omitted the handler runs
   * under the group resolved by the driver.
   */
  groupId?: string;
}

/**
 * Resolved metadata stored on a `@KafkaConsumer` class.
 */
export interface KafkaConsumerMetadata {
  /**
   * Default topic (or pattern) applied to handler methods that do not name their
   * own topic. Optional: a consumer can group handlers that each name their own
   * topic.
   */
  topic?: string;
  options: KafkaConsumerOptions;
}

/**
 * Options accepted by {@link KafkaHandler}.
 */
export interface KafkaHandlerOptions {
  /**
   * Override the consumer group for this single handler. Falls back to the
   * group declared on the owning `@KafkaConsumer`, then to the driver default.
   */
  groupId?: string;
}

/**
 * Resolved metadata stored on a `@KafkaHandler` method.
 */
export interface KafkaHandlerMetadata {
  /**
   * The topic this method consumes. Falls back to the topic declared on the
   * owning `@KafkaConsumer` when omitted.
   */
  topic?: string;
  options: KafkaHandlerOptions;
}

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
