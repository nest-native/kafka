/**
 * Testing utilities for `@nest-native/kafka`.
 *
 * `KafkaTestModule` runs the full transport against an in-memory broker, and the
 * mock-producer helpers let services that inject the producer be unit-tested
 * without a broker or a Nest module. Import them from the
 * `@nest-native/kafka/testing` entrypoint — they are intentionally kept out of
 * the package root so test scaffolding never enters a consumer's production
 * import surface.
 */
export * from './in-memory-kafka-broker';
export * from './mock-kafka-producer';
export * from './kafka-test.module';
