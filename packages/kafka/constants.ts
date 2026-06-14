/**
 * Reflection metadata keys shared by the consumer decorators and the explorer
 * that discovers them. They are intentionally string keys so they survive the
 * `reflect-metadata` round-trip across compilation units.
 */

/**
 * Marks a class as a Kafka consumer. Carries the resolved
 * {@link KafkaConsumerMetadata}.
 */
export const KAFKA_CONSUMER_METADATA = 'nest-native:kafka:consumer';

/**
 * Marks a method as a Kafka handler. Carries the resolved
 * {@link KafkaHandlerMetadata}.
 */
export const KAFKA_HANDLER_METADATA = 'nest-native:kafka:handler';
