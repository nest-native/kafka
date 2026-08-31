import { ModuleMetadata, Provider } from '@nestjs/common';
import {
  KafkaClientConfig,
  KafkaConsumerConfig,
  KafkaDriverFactory,
  KafkaMessageHeaders,
  KafkaProducerConfig,
  KafkaProducerMessage,
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

  /**
   * Opt into request-reply on the **client** side.
   *
   * Absent (the default) nothing about request-reply exists at runtime: no reply
   * consumer is created, no topic is touched, and
   * {@link KafkaRequestReplyService.request} rejects immediately with a
   * configuration error. Configuration *is* the opt-in.
   *
   * The replying side is opted into per handler with
   * `@KafkaHandler(topic, { reply: true })` and needs no `requestReply` block —
   * a replier learns where to answer from the request's own headers. Configure
   * this only to *issue* requests (or to override the header keys an existing
   * replier reads).
   */
  requestReply?: KafkaRequestReplyOptions;
}

/**
 * Client-side request-reply configuration. See ADR 0001 for the routing
 * decision behind the shared reply topic.
 */
export interface KafkaRequestReplyOptions {
  /**
   * The reply topic every instance of this application shares. Provisioned by
   * you, like every topic this package consumes — there is no derived default,
   * because the topic is infrastructure you own.
   *
   * Recommended configuration: any partition count (start with 1 — it is
   * irrelevant to routing), `retention.ms` in the minutes range (a reply older
   * than the longest timeout is garbage by definition), `cleanup.policy=delete`.
   */
  replyTopic: string;

  /**
   * Default per-request timeout. A timeout means the outcome is **unknown** —
   * the request may still be processed — never "it did not happen".
   *
   * @default 30000
   */
  timeoutMs?: number;

  /**
   * How long {@link KafkaRequestReplyService.request} may wait for this
   * instance's reply consumer to be assigned and fetching before failing fast.
   * A missing or unauthorized reply topic surfaces here, named, instead of as a
   * silent hang.
   *
   * @default 10000
   */
  readinessTimeoutMs?: number;

  /**
   * Prefix for the ephemeral per-instance consumer group id; a UUID is appended
   * per process so every instance is a single-member group of its own.
   *
   * @default `${replyTopic}-`
   */
  groupIdPrefix?: string;

  /**
   * Override the header keys the protocol uses. The defaults interoperate with
   * `@nestjs/microservices`; see {@link KafkaRequestReplyHeaderKeys}.
   */
  headers?: Partial<KafkaRequestReplyHeaderKeys>;

  /**
   * Advanced passthrough to the reply consumer — same shape and routing as
   * every other consumer config in this package. The generated `groupId` always
   * wins: a shared group would defeat the routing entirely.
   */
  consumer?: KafkaConsumerConfig;
}

/**
 * The header keys the request-reply protocol reads and writes.
 *
 * The package's documented header neutrality (no standardized
 * `traceId`/`correlationId`/`messageType` keys) is preserved for general
 * messaging and amended for this opt-in feature only: an address and a
 * correlation id have to live somewhere with agreed names. The defaults are
 * `@nestjs/microservices`' own keys so a partially migrated fleet interoperates
 * in both directions without either side knowing the other changed.
 */
export interface KafkaRequestReplyHeaderKeys {
  /** @default 'kafka_correlationId' */
  correlationId: string;
  /** @default 'kafka_replyTopic' */
  replyTopic: string;
  /**
   * Read on the replying side to honour an un-migrated `ClientKafka`'s explicit
   * reply partition. Never written by {@link KafkaRequestReplyService.request}:
   * this package's routing consumes every partition of its reply topic, and
   * `ServerKafka` treats a missing value as "no partition targeting".
   *
   * @default 'kafka_replyPartition'
   */
  replyPartition: string;
  /** @default 'kafka_nest-err' */
  error: string;
  /** @default 'kafka_nest-is-disposed' */
  disposed: string;
}

/**
 * The request {@link KafkaRequestReplyService.request} produces.
 */
export interface KafkaRequestRecord {
  /** The topic the replying handler consumes. */
  topic: string;
  /**
   * The same shape `send()` takes: you serialize `value` explicitly, exactly as
   * for every other produce in this package.
   */
  message: KafkaProducerMessage;
}

/**
 * Per-call overrides for {@link KafkaRequestReplyService.request}.
 */
export interface KafkaRequestOptions {
  /** Overrides `requestReply.timeoutMs` for this call. */
  timeoutMs?: number;
  /**
   * Cancels the *wait*, not the remote work. There is no cross-process
   * cancellation to offer honestly.
   */
  signal?: AbortSignal;
}

/**
 * A correlated reply, as delivered to the instance that issued the request.
 */
export interface KafkaReply<T> {
  /**
   * Deserialized like every consumed payload: JSON when it parses, the decoded
   * string otherwise, `null` for a tombstone.
   */
  value: T;
  headers: KafkaMessageHeaders;
  correlationId: string;
  /** The reply topic the reply was consumed from. */
  topic: string;
  partition: number;
  offset?: string;
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

  /**
   * Reply with the handler's resolved return value when the consumed message
   * carries a reply address in its headers. Fire-and-forget remains the default:
   * a handler answers only because it said so here.
   *
   * The reply value is the handler's **post-enhancer** result — interceptors may
   * transform it, observables collapse to their last value, and a value returned
   * by an exception filter that handled the error becomes the reply. A message
   * with no reply address runs the handler normally and skips the reply step, so
   * replayed requests nobody is waiting on stay legitimate traffic.
   *
   * Incompatible with {@link KafkaHandlerOptions.batch} — a batch has no single
   * request to answer. Declaring both is a bootstrap-time configuration error,
   * as is routing two replying handlers to one topic.
   *
   * @default false
   */
  reply?: boolean;
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
