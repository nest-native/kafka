import type {
  KafkaClientDriver,
  KafkaConsumerConfig,
  KafkaConsumerMessage,
  KafkaDriverConsumer,
  KafkaDriverProducer,
  KafkaEachBatchHandler,
  KafkaEachMessageHandler,
  KafkaProducerMessage,
  KafkaRecordMetadata,
} from '@nest-native/kafka';

interface Registration {
  topics: Set<string>;
  eachMessage?: KafkaEachMessageHandler;
  eachBatch?: KafkaEachBatchHandler;
}

/**
 * A tiny in-memory broker that loops produced messages straight to the
 * consumers subscribed to their topic.
 *
 * It lets the consumer samples — and their smoke tests — run the full
 * `@KafkaConsumer` / `@KafkaHandler` pipeline (guards, interceptors, pipes,
 * filters) without a real Kafka broker or the native `librdkafka` install, in
 * both per-message and batch (`eachBatch`) modes. The real Confluent driver is
 * only used when `KAFKA_BROKERS` is set.
 */
export class InMemoryBroker {
  private readonly consumers: Registration[] = [];

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
    const registration: Registration = { topics: new Set() };
    this.consumers.push(registration);

    return {
      connect: async () => {},
      disconnect: async () => {
        registration.topics.clear();
        registration.eachMessage = undefined;
        registration.eachBatch = undefined;
      },
      subscribe: async subscription => {
        for (const topic of subscription.topics) {
          registration.topics.add(topic);
        }
      },
      run: async config => {
        registration.eachMessage = config.eachMessage;
        registration.eachBatch = config.eachBatch;
      },
    };
  }

  private async deliver(
    topic: string,
    messages: KafkaProducerMessage[],
  ): Promise<void> {
    for (const consumer of this.consumers) {
      if (!consumer.topics.has(topic)) {
        continue;
      }
      await this.deliverToConsumer(consumer, topic, messages);
    }
  }

  private async deliverToConsumer(
    consumer: Registration,
    topic: string,
    messages: KafkaProducerMessage[],
  ): Promise<void> {
    if (consumer.eachBatch) {
      await this.deliverBatch(consumer, topic, messages);
      return;
    }
    await this.deliverEach(consumer, topic, messages);
  }

  private async deliverEach(
    consumer: Registration,
    topic: string,
    messages: KafkaProducerMessage[],
  ): Promise<void> {
    for (let partition = 0; partition < messages.length; partition += 1) {
      // Producers and consumers are decoupled in Kafka: a handler that throws
      // (for example after a guard denies the message and no filter handles the
      // exception) must never fail the producer's send, so each delivery is
      // isolated.
      try {
        await consumer.eachMessage?.({
          topic,
          partition,
          message: toConsumed(messages[partition], partition),
        });
      } catch {
        // Swallowed: the transport's own error mapping decides commit-vs-retry.
      }
    }
  }

  private async deliverBatch(
    consumer: Registration,
    topic: string,
    messages: KafkaProducerMessage[],
  ): Promise<void> {
    const byPartition = groupByPartition(messages);
    const deliveries = [...byPartition].map(([partition, partitionMessages]) =>
      consumer
        .eachBatch?.({
          batch: { topic, partition, messages: partitionMessages },
          resolveOffset: () => {},
        })
        // Isolate each partition's batch the same way as per-message delivery.
        .catch(() => {}),
    );
    await Promise.all(deliveries);
  }
}

function groupByPartition(
  messages: KafkaProducerMessage[],
): Map<number, KafkaConsumerMessage[]> {
  const byPartition = new Map<number, KafkaConsumerMessage[]>();
  for (const message of messages) {
    const partition = message.partition ?? 0;
    const existing = byPartition.get(partition) ?? [];
    existing.push(toConsumed(message, existing.length));
    byPartition.set(partition, existing);
  }
  return byPartition;
}

function toConsumed(
  message: KafkaProducerMessage,
  offset: number,
): KafkaConsumerMessage {
  return {
    key: message.key ?? null,
    value: message.value,
    headers: message.headers,
    offset: String(offset),
  };
}

function metadata(topic: string, partition: number): KafkaRecordMetadata {
  return {
    topicName: topic,
    partition,
    errorCode: 0,
    offset: String(partition),
  };
}
