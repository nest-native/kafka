import { KafkaConsumerBatch, KafkaMessageHeaders } from './driver';

/**
 * An empty, frozen header map returned when a consumed message carries no
 * headers. Shared so every header-less message resolves to the same object
 * instead of allocating a fresh one per message.
 */
const EMPTY_HEADERS: KafkaMessageHeaders = Object.freeze({});

/**
 * A message as it is delivered to a consumer handler, mirroring the
 * KafkaJS-compatible `Message` shape Confluent's client emits. Kept local so the
 * package never imports the optional native peer's types.
 */
export interface KafkaIncomingMessage {
  key?: Buffer | string | null;
  value: Buffer | string | null;
  partition?: number;
  offset?: string;
  timestamp?: string;
  headers?: KafkaMessageHeaders;
}

/**
 * Raw transport context for a single consumed message.
 *
 * It is the object Nest exposes through `ExecutionContext.switchToRpc()
 * .getContext()`, so guards, interceptors, and filters can reach the topic,
 * partition, and the original message. The `@KafkaContext()` parameter decorator
 * (milestone 4) resolves to this same instance.
 *
 * The class deliberately mirrors `@nestjs/microservices`'s `KafkaContext`
 * accessor names (`getTopic`, `getPartition`, `getMessage`) so handlers porting
 * from the official transport keep working.
 */
export class KafkaContext {
  constructor(
    private readonly topic: string,
    private readonly partition: number,
    private readonly message: KafkaIncomingMessage,
  ) {}

  /**
   * The topic the message was consumed from.
   */
  getTopic(): string {
    return this.topic;
  }

  /**
   * The partition the message was consumed from.
   */
  getPartition(): number {
    return this.partition;
  }

  /**
   * The original, undeserialized message.
   */
  getMessage(): KafkaIncomingMessage {
    return this.message;
  }

  /**
   * The headers attached to the message, or an empty (frozen) map when the
   * message carries none. This is the value resolved by the `@KafkaHeaders()`
   * parameter decorator.
   *
   * Header conventions stay neutral on purpose: the package never standardises
   * `traceId` / `correlationId` / `messageType` keys, so the raw header map is
   * returned exactly as Confluent's client delivered it.
   */
  getHeaders(): KafkaMessageHeaders {
    return this.message.headers ?? EMPTY_HEADERS;
  }
}

/**
 * Raw transport context for a batch handler — one consumed topic-partition
 * batch.
 *
 * A `batch: true` `@KafkaHandler` runs once per fetched batch and receives this
 * context through `@KafkaCtx()` (and the raw {@link KafkaConsumerBatch} through
 * `@KafkaBatch()`). It mirrors {@link KafkaContext}'s accessor names for the
 * topic and partition the batch was fetched from, and exposes the raw batch so a
 * handler can reach per-message offsets, keys, and headers.
 */
export class KafkaBatchContext {
  constructor(private readonly batch: KafkaConsumerBatch) {}

  /**
   * The topic the batch was fetched from.
   */
  getTopic(): string {
    return this.batch.topic;
  }

  /**
   * The partition the batch was fetched from.
   */
  getPartition(): number {
    return this.batch.partition;
  }

  /**
   * The raw batch (topic, partition, and the original messages). This is the
   * value resolved by the `@KafkaBatch()` parameter decorator.
   */
  getBatch(): KafkaConsumerBatch {
    return this.batch;
  }
}
