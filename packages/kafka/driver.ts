/**
 * Driver abstraction over Confluent's `@confluentinc/kafka-javascript` client.
 *
 * The published package keeps `"dependencies": {}` and lists the Confluent
 * client as an optional peer. The native `librdkafka` binary is therefore never
 * loaded unless an application actually opens a connection. To honour that
 * constraint the package never imports the client at module-evaluation time;
 * instead it talks to the small surface captured by {@link KafkaClientDriver}
 * and resolves the real client lazily through {@link createConfluentDriver}.
 *
 * The abstraction also makes the producer service fully unit-testable without a
 * broker: tests inject a fake driver through
 * {@link KafkaModuleOptions.driverFactory}.
 */

/**
 * Headers attached to a Kafka message, mirroring the Confluent `IHeaders`
 * shape. Values may be strings, {@link Buffer}s, or arrays of either.
 */
export interface KafkaMessageHeaders {
  [key: string]: Buffer | string | (Buffer | string)[] | undefined;
}

/**
 * A single message handed to the producer, mirroring the Confluent `Message`
 * shape used by the KafkaJS-compatible API.
 */
export interface KafkaProducerMessage {
  key?: Buffer | string | null;
  value: Buffer | string | null;
  partition?: number;
  headers?: KafkaMessageHeaders;
  timestamp?: string;
}

/**
 * A topic/messages pair used by {@link KafkaProducerService.send}.
 */
export interface KafkaSendRecord {
  topic: string;
  messages: KafkaProducerMessage[];
}

/**
 * A topic/messages pair used inside {@link KafkaSendBatch}.
 */
export interface KafkaTopicMessages {
  topic: string;
  messages: KafkaProducerMessage[];
}

/**
 * A batch of per-topic messages published in a single producer call.
 */
export interface KafkaSendBatch {
  topicMessages?: KafkaTopicMessages[];
}

/**
 * Broker acknowledgement metadata returned for each written partition.
 */
export interface KafkaRecordMetadata {
  topicName: string;
  partition: number;
  errorCode: number;
  offset?: string;
  timestamp?: string;
  baseOffset?: string;
  logAppendTime?: string;
  logStartOffset?: string;
}

/**
 * The transactional sub-surface of a producer. A transaction exposes the same
 * `send`/`sendBatch` methods as the producer plus `commit`/`abort`, matching
 * the Confluent `Transaction` type.
 */
export interface KafkaTransaction {
  send(record: KafkaSendRecord): Promise<KafkaRecordMetadata[]>;
  sendBatch(batch: KafkaSendBatch): Promise<KafkaRecordMetadata[]>;
  commit(): Promise<void>;
  abort(): Promise<void>;
}

/**
 * The minimal producer surface the package depends on. This is the subset of
 * the Confluent `Producer` type that the {@link KafkaProducerService} uses.
 */
export interface KafkaDriverProducer {
  connect(): Promise<void>;
  disconnect(): Promise<void>;
  send(record: KafkaSendRecord): Promise<KafkaRecordMetadata[]>;
  sendBatch(batch: KafkaSendBatch): Promise<KafkaRecordMetadata[]>;
  transaction(): Promise<KafkaTransaction>;
}

/**
 * A message handed to a consumer handler, mirroring the KafkaJS-compatible
 * `Message` shape Confluent's client emits per partition.
 */
export interface KafkaConsumerMessage {
  key?: Buffer | string | null;
  value: Buffer | string | null;
  partition?: number;
  offset?: string;
  timestamp?: string;
  headers?: KafkaMessageHeaders;
}

/**
 * The argument the driver passes to the `eachMessage` callback, mirroring the
 * KafkaJS-compatible payload Confluent's client emits: the topic, the partition,
 * and the message.
 */
export interface KafkaEachMessagePayload {
  topic: string;
  partition: number;
  message: KafkaConsumerMessage;
}

/**
 * The per-message callback a consumer runs. Returning resolves the message so
 * the offset can be committed; throwing surfaces the failure to the transport's
 * error handling (milestone 4 maps Nest exceptions onto consumer behaviour).
 */
export type KafkaEachMessageHandler = (
  payload: KafkaEachMessagePayload,
) => Promise<void>;

/**
 * A batch of messages fetched from a single topic-partition, mirroring the
 * KafkaJS-compatible `batch` object Confluent's client hands to `eachBatch`.
 * Only the fields the transport relies on are modelled; advanced fields the
 * client provides (`highWatermark`, `offsetLag`, …) pass through untouched.
 */
export interface KafkaConsumerBatch {
  topic: string;
  partition: number;
  messages: KafkaConsumerMessage[];
}

/**
 * The argument the driver passes to the `eachBatch` callback, mirroring the
 * KafkaJS-compatible payload Confluent's client emits: the fetched
 * {@link KafkaConsumerBatch} plus the per-message offset-resolution callback used
 * to commit progress incrementally.
 */
export interface KafkaEachBatchPayload {
  batch: KafkaConsumerBatch;
  /**
   * Mark one message in the batch as processed so its offset can be committed.
   *
   * Resolving offsets per message — rather than only at the end of the batch —
   * is what makes batch consumption rebalance-safe (`nestjs/nest#12355`): a
   * partition revoked mid-batch keeps the offsets already resolved, so the next
   * owner resumes after the last processed message instead of replaying the
   * whole batch or hanging.
   */
  resolveOffset: (offset: string) => void;
}

/**
 * The per-batch callback a consumer runs when a handler opts into batch
 * consumption. Returning resolves the batch (offsets up to the last resolved
 * message commit); throwing surfaces the failure to the transport's error
 * handling.
 */
export type KafkaEachBatchHandler = (
  payload: KafkaEachBatchPayload,
) => Promise<void>;

/**
 * Subscription request forwarded to the consumer, mirroring the
 * KafkaJS-compatible `subscribe` options.
 */
export interface KafkaSubscription {
  topics: string[];
  fromBeginning?: boolean;
}

/**
 * Runtime configuration for {@link KafkaDriverConsumer.run}, mirroring the
 * KafkaJS-compatible `run` options the package relies on. A consumer runs either
 * `eachMessage` (the default, one message at a time) or `eachBatch` (when a
 * handler opts into batch consumption); the two never run on the same consumer.
 */
export interface KafkaConsumerRunConfig {
  eachMessage?: KafkaEachMessageHandler;
  eachBatch?: KafkaEachBatchHandler;

  /**
   * How many partitions the consumer processes concurrently. `1` (the default)
   * keeps strict per-partition ordering; a higher value lets messages from
   * different partitions run at the same time, addressing the sequential
   * per-topic processing of the official transport (`nestjs/nest#12703`).
   * Ordering within a single partition is always preserved.
   */
  partitionsConsumedConcurrently?: number;

  /**
   * When `true` (the default for `eachBatch`) the client commits the batch's
   * last offset automatically once the callback returns. The transport disables
   * it and resolves offsets per message instead so a rebalance mid-batch never
   * loses or replays processed messages.
   */
  eachBatchAutoResolve?: boolean;
}

/**
 * The minimal consumer surface the package depends on. This is the subset of the
 * Confluent `Consumer` type the transport uses to subscribe to topics and
 * dispatch messages through the Nest enhancer pipeline.
 */
export interface KafkaDriverConsumer {
  connect(): Promise<void>;
  disconnect(): Promise<void>;
  subscribe(subscription: KafkaSubscription): Promise<void>;
  run(config: KafkaConsumerRunConfig): Promise<void>;
}

/**
 * Per-consumer configuration forwarded to `kafka.consumer(...)`. The `groupId`
 * is the only field the package models; everything else passes straight through
 * so advanced Confluent options are available without the package modelling
 * them.
 */
export interface KafkaConsumerConfig {
  groupId?: string;
  [option: string]: unknown;
}

/**
 * The driver the module wires into the producer service and the consumer
 * transport. A driver knows how to create producers and consumers.
 */
export interface KafkaClientDriver {
  /**
   * Create a producer bound to the configured broker connection.
   */
  createProducer(): KafkaDriverProducer;

  /**
   * Create a consumer bound to the configured broker connection. The optional
   * `config` carries the resolved consumer group and any advanced Confluent
   * options.
   */
  createConsumer(config?: KafkaConsumerConfig): KafkaDriverConsumer;
}

/**
 * Connection configuration forwarded to the Confluent `Kafka` constructor. The
 * `brokers` list is required; everything else passes straight through so
 * advanced users can supply SASL/SSL or any other Confluent option without the
 * package having to model it.
 */
export interface KafkaClientConfig {
  brokers: string[];
  clientId?: string;
  [option: string]: unknown;
}

/**
 * Producer configuration forwarded to `kafka.producer(...)`. Left intentionally
 * open so advanced Confluent options pass through untouched.
 */
export interface KafkaProducerConfig {
  [option: string]: unknown;
}

/**
 * Factory that builds a {@link KafkaClientDriver} from the resolved client and
 * producer configuration. Override it through
 * {@link KafkaModuleOptions.driverFactory} to inject a fake driver in tests or
 * to plug in a custom client.
 */
export type KafkaDriverFactory = (
  clientConfig: KafkaClientConfig,
  producerConfig: KafkaProducerConfig,
) => KafkaClientDriver;

/**
 * Shape of the `Kafka` class exported by `@confluentinc/kafka-javascript`,
 * captured locally so the package never has to import the optional peer's
 * types.
 */
interface ConfluentKafka {
  producer(config?: Record<string, unknown>): KafkaDriverProducer;
  consumer(config?: Record<string, unknown>): KafkaDriverConsumer;
}

interface ConfluentKafkaConstructor {
  new (config?: Record<string, unknown>): ConfluentKafka;
}

interface ConfluentKafkaModule {
  KafkaJS: { Kafka: ConfluentKafkaConstructor };
}

/**
 * Default driver factory. Lazily resolves Confluent's client only when a driver
 * is actually constructed, so importing the package never loads `librdkafka`.
 *
 * The Confluent constructor expects connection options nested under a `kafkaJS`
 * key; the configuration supplied to {@link KafkaModuleOptions.client} and
 * {@link KafkaModuleOptions.producer} is forwarded there verbatim.
 */
export const createConfluentDriver: KafkaDriverFactory = (
  clientConfig,
  producerConfig,
) => {
  const { KafkaJS } = loadConfluentModule();
  const kafka = new KafkaJS.Kafka({ kafkaJS: { ...clientConfig } });

  return {
    createProducer: () => kafka.producer({ kafkaJS: { ...producerConfig } }),
    createConsumer: (consumerConfig = {}) =>
      kafka.consumer({ kafkaJS: { ...consumerConfig } }),
  };
};

function loadConfluentModule(): ConfluentKafkaModule {
  try {
    return require('@confluentinc/kafka-javascript') as ConfluentKafkaModule;
  } catch (cause) {
    const error = new Error(
      'The optional peer "@confluentinc/kafka-javascript" is not installed. ' +
        'Install it to open a real Kafka connection, or supply a custom ' +
        '"driverFactory" through KafkaModule.forRoot for tests.',
    );
    (error as { cause?: unknown }).cause = cause;
    throw error;
  }
}
