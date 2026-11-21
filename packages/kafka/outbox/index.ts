/**
 * `@nest-native/kafka/outbox`: a Kafka transport for the official
 * `@nestjs/outbox` relay, publishing with this package's producer.
 *
 * It is an entrypoint of its own, never re-exported from the package root,
 * because it imports `@nestjs/outbox`, an optional peer: an application that
 * does not use the outbox never loads it.
 */
export { KafkaOutboxTransport } from './kafka-outbox.transport';
export type {
  KafkaOutboxRecord,
  KafkaOutboxTransportOptions,
} from './kafka-outbox.transport';
