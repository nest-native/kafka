/**
 * Injection tokens shared across the package.
 */

/**
 * Token for the resolved {@link KafkaModuleOptions}.
 *
 * Providers that need the global Kafka configuration inject this token. The
 * producer service and the consumer building blocks added in later milestones
 * resolve their defaults from the same provider.
 */
export const KAFKA_MODULE_OPTIONS = Symbol('KAFKA_MODULE_OPTIONS');

/**
 * Token for the resolved {@link KafkaClientDriver}.
 */
export const KAFKA_CLIENT_DRIVER = Symbol('KAFKA_CLIENT_DRIVER');

/**
 * Token for the raw producer exposed through {@link InjectKafkaProducer}.
 */
export const KAFKA_PRODUCER = Symbol('KAFKA_PRODUCER');

/**
 * Token for the {@link InMemoryKafkaBroker} backing {@link KafkaTestModule}.
 *
 * Inject it (or use `@InjectKafkaTestBroker()`) in a test to inspect produced
 * messages and inject consumed ones.
 */
export const KAFKA_TEST_BROKER = Symbol('KAFKA_TEST_BROKER');
