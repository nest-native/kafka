import {
  createConfluentDriver,
  type KafkaDriverFactory,
} from '@nest-native/kafka';
import { InMemoryBroker } from './in-memory-broker';

/**
 * Brokers parsed from `KAFKA_BROKERS`, or an empty list when unset. Empty means
 * the sample runs entirely in memory — no broker, no native `librdkafka`.
 */
export function resolveBrokers(): string[] {
  return (process.env.KAFKA_BROKERS ?? '')
    .split(',')
    .map(broker => broker.trim())
    .filter(broker => broker.length > 0);
}

/**
 * Pick the driver factory for the current environment: the real Confluent driver
 * when `KAFKA_BROKERS` is set, otherwise the in-memory transactional broker.
 */
export function resolveDriverFactory(broker: InMemoryBroker): KafkaDriverFactory {
  return resolveBrokers().length > 0
    ? createConfluentDriver
    : () => broker.createDriver();
}
