import { KafkaMessageHeaders } from './driver';

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
}
