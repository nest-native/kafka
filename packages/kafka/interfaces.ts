import { ModuleMetadata, Provider } from '@nestjs/common';
import {
  KafkaClientConfig,
  KafkaDriverFactory,
  KafkaProducerConfig,
} from './driver';
import { KafkaErrorMapper } from './kafka-error-mapping';

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

  /**
   * Map an unhandled handler error to consumer behaviour (commit the offset or
   * retry by redelivery). Defaults to {@link defaultKafkaErrorMapper}, which
   * commits 4xx-style client errors and retries everything else.
   *
   * Only errors that escape the handler's `@UseFilters` exception filters reach
   * this mapper, so an application can still acknowledge any error by catching
   * it in a filter.
   */
  errorMapper?: KafkaErrorMapper;

  /**
   * Default partition concurrency for every consumer the module starts, unless a
   * `@KafkaConsumer` or `@KafkaHandler` overrides it. `1` (the default) keeps
   * strict per-partition ordering; raising it lets partitions process
   * concurrently, addressing the official transport's sequential per-topic
   * processing (`nestjs/nest#12703`). See {@link KafkaConcurrencyOptions}.
   *
   * @default 1
   */
  concurrency?: number;

  /**
   * Default backpressure cap — the maximum number of messages any one consumer
   * processes at once — unless a `@KafkaConsumer` or `@KafkaHandler` overrides
   * it. Caps in-flight work so a fast broker cannot overwhelm slow handlers
   * (BRIEF §9 backpressure). `0` or a negative value disables the cap.
   *
   * @default 0 (uncapped)
   */
  maxInFlight?: number;
}

/**
 * The concurrency and backpressure controls a `@KafkaConsumer` or
 * `@KafkaHandler` may set. They are resolved handler → consumer → module so a
 * single handler can opt out of (or into) the module-wide default.
 */
export interface KafkaConcurrencyOptions {
  /**
   * How many partitions this consumer processes concurrently. `1` keeps strict
   * per-partition ordering; a higher value processes partitions concurrently
   * (the documented opt-out of the sequential per-topic processing in
   * `nestjs/nest#12703`). Ordering within a partition is always preserved.
   */
  concurrency?: number;

  /**
   * The maximum number of messages this consumer processes at once. Caps
   * in-flight work for backpressure; `0` or a negative value disables the cap.
   */
  maxInFlight?: number;
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
export interface KafkaConsumerOptions extends KafkaConcurrencyOptions {
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
export interface KafkaHandlerOptions extends KafkaConcurrencyOptions {
  /**
   * Override the consumer group for this single handler. Falls back to the
   * group declared on the owning `@KafkaConsumer`, then to the driver default.
   */
  groupId?: string;

  /**
   * Consume messages in batches instead of one at a time. A batch handler is
   * invoked once per fetched topic-partition batch and receives the array of
   * deserialized payloads (via `@KafkaMessage()`) or the raw
   * {@link KafkaConsumerBatch} (via `@KafkaBatch()`). Offsets resolve per message
   * so a rebalance mid-batch stays safe (`nestjs/nest#12355`).
   *
   * A batch handler runs on its own consumer: per-message and batch handlers are
   * never mixed on a single Kafka consumer instance.
   *
   * @default false
   */
  batch?: boolean;
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
