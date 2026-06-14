import type {
  KafkaClientDriver,
  KafkaConsumerConfig,
  KafkaDriverConsumer,
  KafkaDriverProducer,
  KafkaProducerMessage,
  KafkaEachMessageHandler,
  KafkaRecordMetadata,
} from '@nest-native/kafka';

/**
 * A tiny in-memory broker that loops produced messages straight to the consumers
 * subscribed to their topic, forwarding headers so the `@KafkaHeaders()`
 * decorator has something to read.
 *
 * Unlike the showcase broker it surfaces a delivery error back to the producer's
 * caller, so the smoke test can observe the transport's error mapping: a message
 * the default mapper retries rethrows here (the offset would stay uncommitted on
 * a real broker), while one the mapper commits resolves cleanly.
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
      sendBatch: async () => [],
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
        // A retried error rethrows here, mirroring a real broker leaving the
        // offset uncommitted; a committed error resolves cleanly.
        await consumer.eachMessage({
          topic,
          partition,
          message: {
            key: messages[partition].key ?? null,
            value: messages[partition].value,
            headers: messages[partition].headers,
          },
        });
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
