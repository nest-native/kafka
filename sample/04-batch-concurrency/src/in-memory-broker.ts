import type {
  KafkaClientDriver,
  KafkaConsumerConfig,
  KafkaConsumerMessage,
  KafkaDriverConsumer,
  KafkaDriverProducer,
  KafkaEachBatchHandler,
  KafkaProducerMessage,
  KafkaRecordMetadata,
} from '@nest-native/kafka';

interface Registration {
  topics: Set<string>;
  eachBatch?: KafkaEachBatchHandler;
}

/**
 * A tiny in-memory broker that groups produced messages by partition and hands
 * each partition's messages to a batch consumer's `eachBatch` callback — enough
 * to exercise batch consumption and per-partition concurrency without a real
 * Kafka broker or the native `librdkafka` install.
 *
 * It tracks which offsets the transport resolved so the smoke test can prove the
 * per-message offset resolution that makes batch consumption rebalance-safe
 * (`nestjs/nest#12355`): a partition revoked mid-batch keeps the resolved
 * offsets instead of replaying the whole batch.
 */
export class InMemoryBroker {
  private readonly consumers: Registration[] = [];

  /** Offsets the transport resolved, by topic-partition, for assertions. */
  readonly resolvedOffsets = new Map<string, string[]>();

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
    const registration: Registration = { topics: new Set() };
    this.consumers.push(registration);

    return {
      connect: async () => {},
      disconnect: async () => {
        registration.topics.clear();
        registration.eachBatch = undefined;
      },
      subscribe: async subscription => {
        for (const topic of subscription.topics) {
          registration.topics.add(topic);
        }
      },
      run: async config => {
        registration.eachBatch = config.eachBatch;
      },
    };
  }

  /**
   * Deliver produced messages as one batch per partition. Each produced message
   * lands on a partition keyed by its declared `partition` (default 0), so a test
   * producing across partitions exercises per-partition concurrency.
   */
  private async deliver(
    topic: string,
    messages: KafkaProducerMessage[],
  ): Promise<void> {
    const byPartition = groupByPartition(messages);
    for (const consumer of this.consumers) {
      if (!consumer.eachBatch || !consumer.topics.has(topic)) {
        continue;
      }
      await this.deliverToConsumer(consumer, topic, byPartition);
    }
  }

  private async deliverToConsumer(
    consumer: Registration,
    topic: string,
    byPartition: Map<number, KafkaConsumerMessage[]>,
  ): Promise<void> {
    const deliveries = [...byPartition].map(([partition, partitionMessages]) =>
      consumer.eachBatch?.({
        batch: { topic, partition, messages: partitionMessages },
        resolveOffset: offset => {
          const key = `${topic}-${partition}`;
          const offsets = this.resolvedOffsets.get(key) ?? [];
          offsets.push(offset);
          this.resolvedOffsets.set(key, offsets);
        },
      }),
    );
    // Partitions are delivered concurrently, mirroring a broker that fetches
    // several partitions at once; the transport keeps ordering within each.
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
    // Offsets are per partition in Kafka, so each partition starts its own
    // sequence at 0 — the length of the bucket so far.
    existing.push({
      key: message.key ?? null,
      value: message.value,
      headers: message.headers,
      offset: String(existing.length),
    });
    byPartition.set(partition, existing);
  }
  return byPartition;
}

function metadata(topic: string, partition: number): KafkaRecordMetadata {
  return {
    topicName: topic,
    partition,
    errorCode: 0,
    offset: String(partition),
  };
}
