import type {
  KafkaClientDriver,
  KafkaDriverFactory,
  KafkaDriverProducer,
  KafkaProducerMessage,
  KafkaRecordMetadata,
} from '@nest-native/kafka';
import type { LoggingMessageHandler } from './logging-message.handler';

/**
 * Build a driver factory whose producers loop every published message straight
 * back to the {@link LoggingMessageHandler}.
 *
 * This keeps the sample (and its smoke test) runnable without a Kafka broker,
 * which is exactly the "skip locally if env missing" contract the project
 * follows: the real Confluent driver is only used when `KAFKA_BROKERS` is set
 * (see `kafka-driver.ts`).
 */
export function createInMemoryDriverFactory(
  handler: LoggingMessageHandler,
): KafkaDriverFactory {
  return () => {
    const driver: KafkaClientDriver = {
      createProducer: () => createInMemoryProducer(handler),
    };
    return driver;
  };
}

function createInMemoryProducer(
  handler: LoggingMessageHandler,
): KafkaDriverProducer {
  const deliver = (topic: string, messages: KafkaProducerMessage[]): void => {
    for (const message of messages) {
      handler.handle(topic, decodeValue(message.value));
    }
  };

  return {
    connect: async () => {},
    disconnect: async () => {},
    send: async record => {
      deliver(record.topic, record.messages);
      return record.messages.map((_, index) => metadata(record.topic, index));
    },
    sendBatch: async batch => {
      const results: KafkaRecordMetadata[] = [];
      for (const topicMessages of batch.topicMessages ?? []) {
        deliver(topicMessages.topic, topicMessages.messages);
        topicMessages.messages.forEach((_, index) =>
          results.push(metadata(topicMessages.topic, index)),
        );
      }
      return results;
    },
    transaction: async () => {
      throw new Error('Transactions arrive in a later milestone.');
    },
  };
}

function decodeValue(value: KafkaProducerMessage['value']): string {
  if (value === null) {
    return '';
  }
  return Buffer.isBuffer(value) ? value.toString('utf8') : value;
}

function metadata(topic: string, partition: number): KafkaRecordMetadata {
  return { topicName: topic, partition, errorCode: 0, offset: String(partition) };
}
