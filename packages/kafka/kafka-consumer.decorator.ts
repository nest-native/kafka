import { Injectable, SetMetadata } from '@nestjs/common';
import { KAFKA_CONSUMER_METADATA } from './constants';
import { KafkaConsumerMetadata, KafkaConsumerOptions } from './interfaces';

/**
 * Mark a class as a Kafka consumer.
 *
 * Mirrors the ergonomics of a Nest controller for the Kafka transport: the
 * class becomes injectable and the {@link KafkaHandler}-decorated methods on it
 * are discovered, wired through the full Nest enhancer pipeline (guards,
 * interceptors, pipes, filters), and subscribed to their topics.
 *
 * The optional `topic` acts as a default for handler methods that do not name
 * their own topic — useful when one class groups several handlers for the same
 * topic. Leaving it unset requires every {@link KafkaHandler} to name its topic.
 *
 * @example
 * ```ts
 * @KafkaConsumer('orders', { groupId: 'orders-service' })
 * export class OrdersConsumer {
 *   @KafkaHandler()
 *   handle(@KafkaMessage() order: OrderPlaced) {}
 * }
 * ```
 *
 * @param topic - Optional default topic (or pattern) for the class's handlers.
 * @param options - Optional consumer-group configuration.
 *
 * @publicApi
 */
export function KafkaConsumer(
  topic?: string,
  options: KafkaConsumerOptions = {},
): ClassDecorator {
  const metadata: KafkaConsumerMetadata = { topic, options };

  return (target: object) => {
    Injectable()(target as Parameters<ClassDecorator>[0]);
    SetMetadata(KAFKA_CONSUMER_METADATA, metadata)(
      target as Parameters<ClassDecorator>[0],
    );
  };
}
