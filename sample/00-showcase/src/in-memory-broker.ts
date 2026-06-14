import type {
  KafkaClientDriver,
  KafkaConsumerConfig,
  KafkaDriverConsumer,
  KafkaDriverProducer,
  KafkaEachMessageHandler,
  KafkaProducerMessage,
  KafkaRecordMetadata,
} from '@nest-native/kafka';

/**
 * A tiny in-memory broker that loops produced messages straight to the
 * consumers subscribed to their topic.
 *
 * It lets the consumer samples — and their smoke tests — run the full
 * `@KafkaConsumer` / `@KafkaHandler` pipeline (guards, interceptors, pipes,
 * filters) without a real Kafka broker or the native `librdkafka` install. The
 * real Confluent driver is only used when `KAFKA_BROKERS` is set.
 */
export class InMemoryBroker {
  private readonly consumers: {
    topics: Set<string>;
    eachMessage?: KafkaEachMessageHandler;
  }[] = [];

  createDriver(): KafkaClientDriver {
    return {
      createProducer: () => this.createProducer(),
      createConsumer: (config?: KafkaConsumerConfig) =>
        this.createConsumer(config),
    };
  }

  private createProducer(): KafkaDriverProducer {
    return {
      connect: async () => {},
      disconnect: async () => {},
      send: async record => {
        await this.deliver(record.topic, record.messages);
        return record.messages.map((_, index) =>
          metadata(record.topic, index),
        );
      },
      sendBatch: async batch => {
        const results: KafkaRecordMetadata[] = [];
        for (const topicMessages of batch.topicMessages ?? []) {
          await this.deliver(topicMessages.topic, topicMessages.messages);
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

  private createConsumer(_config?: KafkaConsumerConfig): KafkaDriverConsumer {
    const registration: {
      topics: Set<string>;
      eachMessage?: KafkaEachMessageHandler;
    } = { topics: new Set() };
    this.consumers.push(registration);

    return {
      connect: async () => {},
      disconnect: async () => {
        registration.topics.clear();
        registration.eachMessage = undefined;
      },
      subscribe: async subscription => {
        for (const topic of subscription.topics) {
          registration.topics.add(topic);
        }
      },
      run: async config => {
        registration.eachMessage = config.eachMessage;
      },
    };
  }

  private async deliver(
    topic: string,
    messages: KafkaProducerMessage[],
  ): Promise<void> {
    for (const consumer of this.consumers) {
      if (!consumer.eachMessage || !consumer.topics.has(topic)) {
        continue;
      }
      for (let partition = 0; partition < messages.length; partition += 1) {
        // Producers and consumers are decoupled in Kafka: a handler that throws
        // (for example after a guard denies the message and no filter handles
        // the exception) must never fail the producer's send. The loopback keeps
        // that contract by isolating each delivery.
        try {
          await consumer.eachMessage({
            topic,
            partition,
            message: { value: messages[partition].value },
          });
        } catch {
          // Swallowed: milestone 4 introduces explicit error mapping and retries.
        }
      }
    }
  }
}

function metadata(topic: string, partition: number): KafkaRecordMetadata {
  return {
    topicName: topic,
    partition,
    errorCode: 0,
    offset: String(partition),
  };
}
