import { Inject, Injectable, Optional, type Type } from '@nestjs/common';
import {
  type OutboxEnvelope,
  type OutboxMessage,
  OutboxTransport,
} from '@nestjs/outbox';
import type { KafkaProducerMessage, KafkaSendRecord } from '../driver';
import { KafkaProducerService } from '../kafka-producer.service';
import { toPermanentFailure } from './kafka-outbox.errors';

/**
 * What {@link KafkaOutboxTransportOptions.toRecord} can change in the record the
 * transport produces. The fields are named as in the producer's own types:
 * `topic` as in `KafkaSendRecord`, the others as in `KafkaProducerMessage`.
 *
 * @publicApi
 */
export interface KafkaOutboxRecord {
  /** The topic to produce to. Default: the message's topic. */
  topic?: string;
  /** The record key. Default: the message's key, `null` when it has none. */
  key?: KafkaProducerMessage['key'];
  /** The record value. Default: the JSON of the message's `OutboxEnvelope`. */
  value?: KafkaProducerMessage['value'];
  /** The record headers, in place of the default ones: the message's headers. */
  headers?: KafkaProducerMessage['headers'];
  /** A partition to produce to. Default: none, so the key picks the partition. */
  partition?: KafkaProducerMessage['partition'];
}

/**
 * Options for {@link KafkaOutboxTransport}.
 *
 * @publicApi
 */
export interface KafkaOutboxTransportOptions {
  /**
   * Maps a message to the record to produce, for a wire other than the default.
   * Return only what differs from the default: a field left out (or
   * `undefined`) keeps it, so returning `{ topic }` alone changes the topic and
   * nothing else. `headers` replaces the default headers as a whole; spread
   * `message.headers` into it to keep them.
   *
   * `envelope` is the `OutboxEnvelope` the default value is the JSON of. The
   * function is called with no `this`. Throw `NonRetryableMessageError` from it
   * to dead-letter a message it cannot map; anything else it throws fails the
   * attempt, which the relay retries.
   */
  toRecord?: (
    message: OutboxMessage,
    envelope: OutboxEnvelope,
  ) => KafkaOutboxRecord | undefined;
}

/**
 * Never provided. It stands for the constructor's second argument, so that when
 * Nest builds the transport (the class in `OutboxModule.forRoot`'s `transports`)
 * it passes `undefined` there, instead of failing to resolve an interface.
 */
const NO_OPTIONS = Symbol('KafkaOutboxTransportOptions');

/**
 * An `OutboxTransport` for the official `@nestjs/outbox` relay that publishes
 * with this package's `KafkaProducerService`, on Confluent's client.
 *
 * **The wire.** By default it produces the record the official
 * `ClientProxyTransport` produces through a `ClientKafka` (its `toKafkaRecord`,
 * since `@nestjs/outbox` 0.1.3): to the message's topic, under its key (`null`
 * when it has none) and headers, with the JSON of its `OutboxEnvelope` (`id`,
 * `topic`, `key`, `headers`, `createdAt`, `payload`) as the value, byte for byte.
 * A consumer written for that path keeps working when an application swaps
 * transports. `toRecord` changes the wire.
 *
 * **Outcomes.** Resolving means the broker acknowledged the record, under the
 * producer's `acks`. A failure a retry can never fix (the record is too large,
 * the topic name is invalid or not writable by this client, the broker refused
 * the record itself) is thrown as `NonRetryableMessageError`, with the client's
 * error as its `cause`, so the relay dead-letters the message at once. Any other
 * failure is thrown as it is, and the relay retries it with backoff.
 *
 * **Delivery is at-least-once.** A produce can't be called back: the transport
 * checks the relay's `signal` before producing, but once the record is handed to
 * the client, it is delivered or fails on the client's schedule
 * (`message.timeout.ms`). A send that outlives `relay.publishTimeout` counts as a
 * failed attempt and may still land next to its retry. Consumers deduplicate by
 * the envelope's `id`, for example with `OutboxInbox`.
 *
 * Register it by name in `OutboxModule`'s `transports`: the class, which Nest
 * builds with the `KafkaProducerService` of the (global) `KafkaModule`;
 * `KafkaOutboxTransport.with(options)`, the class configured with options; or an
 * instance built in `forRootAsync()`'s factory.
 *
 * @example
 * ```ts
 * OutboxModule.forRoot({ transports: { kafka: KafkaOutboxTransport } });
 *
 * OutboxModule.forRootAsync({
 *   inject: [KafkaProducerService],
 *   useFactory: (producer: KafkaProducerService) => ({
 *     transports: { kafka: new KafkaOutboxTransport(producer, { toRecord }) },
 *   }),
 * });
 * ```
 *
 * @publicApi
 */
@Injectable()
export class KafkaOutboxTransport extends OutboxTransport {
  private readonly toRecord: KafkaOutboxTransportOptions['toRecord'];

  constructor(
    private readonly producer: KafkaProducerService,
    @Optional() @Inject(NO_OPTIONS) options: KafkaOutboxTransportOptions = {},
  ) {
    super();
    this.toRecord = options.toRecord;
  }

  /**
   * The transport as a class configured with `options`, for
   * `OutboxModule.forRoot({ transports })`, where Nest builds the class and
   * would pass it no options.
   *
   * @example
   * ```ts
   * OutboxModule.forRoot({
   *   transports: {
   *     kafka: KafkaOutboxTransport.with({
   *       toRecord: message => ({ topic: `${prefix}${message.topic}` }),
   *     }),
   *   },
   * });
   * ```
   */
  static with(options: KafkaOutboxTransportOptions): Type<KafkaOutboxTransport> {
    @Injectable()
    class ConfiguredKafkaOutboxTransport extends KafkaOutboxTransport {
      constructor(producer: KafkaProducerService) {
        super(producer, options);
      }
    }
    return ConfiguredKafkaOutboxTransport;
  }

  /**
   * Produce `message` and resolve once the broker acknowledged it.
   */
  async publish(
    message: OutboxMessage,
    { signal }: { signal: AbortSignal },
  ): Promise<void> {
    // The last point where the relay's timeout can stop the attempt: once the
    // record is handed to the client, it is delivered or fails on its own.
    signal.throwIfAborted();
    const record = this.recordFor(message);
    try {
      await this.producer.send(record);
    } catch (error) {
      throw toPermanentFailure(error, record.topic) ?? error;
    }
  }

  private recordFor(message: OutboxMessage): KafkaSendRecord {
    const envelope = toEnvelope(message);
    const { toRecord } = this;
    const custom = toRecord?.(message, envelope) ?? {};
    const produced: KafkaProducerMessage = {
      key: custom.key === undefined ? message.key : custom.key,
      value:
        custom.value === undefined ? JSON.stringify(envelope) : custom.value,
      headers: custom.headers ?? { ...message.headers },
    };
    if (custom.partition !== undefined) {
      produced.partition = custom.partition;
    }
    return { topic: custom.topic ?? message.topic, messages: [produced] };
  }
}

/**
 * The envelope the official `ClientProxyTransport` sends, with its fields in the
 * same order, so that its JSON is the same, byte for byte.
 */
function toEnvelope(message: OutboxMessage): OutboxEnvelope {
  return {
    id: message.id,
    topic: message.topic,
    key: message.key,
    headers: message.headers,
    createdAt: message.createdAt,
    payload: message.payload,
  };
}
