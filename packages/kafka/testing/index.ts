/**
 * Testing utilities for `@nest-native/kafka`.
 *
 * `KafkaTestModule` runs the full transport against an in-memory broker, and the
 * mock-producer helpers let services that inject the producer be unit-tested
 * without a broker or a Nest module. All of it is re-exported from the package
 * root, so applications import from `@nest-native/kafka` directly.
 */
export * from './in-memory-kafka-broker';
export * from './mock-kafka-producer';
export * from './kafka-test.module';
