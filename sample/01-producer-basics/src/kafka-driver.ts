import {
  createConfluentDriver,
  type KafkaDriverFactory,
} from '@nest-native/kafka';
import { createInMemoryDriverFactory } from './in-memory-driver';
import type { LoggingMessageHandler } from './logging-message.handler';

/**
 * Brokers parsed from the `KAFKA_BROKERS` environment variable, or an empty
 * list when it is unset. When empty the sample runs entirely in memory so it
 * needs no broker and no native `librdkafka` install.
 */
export function resolveBrokers(): string[] {
  return (process.env.KAFKA_BROKERS ?? '')
    .split(',')
    .map(broker => broker.trim())
    .filter(broker => broker.length > 0);
}

/**
 * Pick the driver factory for the current environment: the real Confluent
 * driver when `KAFKA_BROKERS` is set, otherwise the in-memory loopback driver.
 */
export function resolveDriverFactory(
  handler: LoggingMessageHandler,
): KafkaDriverFactory {
  return resolveBrokers().length > 0
    ? createConfluentDriver
    : createInMemoryDriverFactory(handler);
}
