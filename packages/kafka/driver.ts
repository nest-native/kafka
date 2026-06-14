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
 * The driver the module wires into the producer service. A driver knows how to
 * create producers; later milestones extend it with consumer creation.
 */
export interface KafkaClientDriver {
  /**
   * Create a producer bound to the configured broker connection.
   */
  createProducer(): KafkaDriverProducer;
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
