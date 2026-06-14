import type {
  KafkaClientDriver,
  KafkaConsumerConfig,
  KafkaDriverConsumer,
  KafkaDriverProducer,
  KafkaProducerMessage,
  KafkaRecordMetadata,
  KafkaSendBatch,
  KafkaSendRecord,
  KafkaTransaction,
  KafkaTransactionOffsets,
} from '@nest-native/kafka';

/** One delivered record, captured so the smoke test can assert on it. */
export interface DeliveredRecord {
  topic: string;
  key: string | null;
  value: string;
}

/** One offsets-commit recorded by a transaction's `sendOffsets`. */
export interface RecordedOffsets {
  group: string;
  topics: KafkaTransactionOffsets['topics'];
}

/**
 * An in-memory broker that models Kafka transaction atomicity: a transactional
 * producer buffers everything it writes and only delivers it on `commit`; an
 * `abort` discards the buffer so nothing is ever delivered. Offsets passed to
 * `sendOffsets` are committed with the same all-or-nothing rule.
 *
 * It keeps the sample (and its smoke test) runnable without a real Kafka broker
 * or the native `librdkafka` install — the "skip locally if env missing"
 * contract the project follows. The real Confluent driver is used only when
 * `KAFKA_BROKERS` is set (see `kafka-driver.ts`).
 */
export class InMemoryBroker {
  readonly delivered: DeliveredRecord[] = [];
  readonly committedOffsets: RecordedOffsets[] = [];

  createDriver(): KafkaClientDriver {
    return {
      createProducer: () => this.createProducer(),
      // A transactional producer needs a consumer group to attribute offsets to;
      // this sample never actually consumes, so the consumer is a no-op whose
      // only job is to carry the resolved groupId into `sendOffsets`.
      createConsumer: (config?: KafkaConsumerConfig) =>
        this.createConsumer(config),
    };
  }

  private createProducer(): KafkaDriverProducer {
    const deliverRecord = (record: KafkaSendRecord): KafkaRecordMetadata[] => {
      this.deliver(record.topic, record.messages);
      return record.messages.map((_, index) => metadata(record.topic, index));
    };

    const deliverBatch = (batch: KafkaSendBatch): KafkaRecordMetadata[] => {
      const results: KafkaRecordMetadata[] = [];
      for (const topicMessages of batch.topicMessages ?? []) {
        this.deliver(topicMessages.topic, topicMessages.messages);
        topicMessages.messages.forEach((_, index) =>
          results.push(metadata(topicMessages.topic, index)),
        );
      }
      return results;
    };

    return {
      connect: async () => {},
      disconnect: async () => {},
      // A transactional producer must not send outside a transaction, so the
      // direct send/sendBatch are unused here. Modelled for completeness.
      send: async record => deliverRecord(record),
      sendBatch: async batch => deliverBatch(batch),
      transaction: async () => this.createTransaction(deliverRecord, deliverBatch),
    };
  }

  private createTransaction(
    deliverRecord: (record: KafkaSendRecord) => KafkaRecordMetadata[],
    deliverBatch: (batch: KafkaSendBatch) => KafkaRecordMetadata[],
  ): KafkaTransaction {
    // Buffer everything until commit so an abort delivers nothing.
    const pendingRecords: KafkaSendRecord[] = [];
    const pendingBatches: KafkaSendBatch[] = [];
    let pendingOffsets: RecordedOffsets | undefined;

    return {
      send: async record => {
        pendingRecords.push(record);
        return record.messages.map((_, index) => metadata(record.topic, index));
      },
      sendBatch: async batch => {
        pendingBatches.push(batch);
        return [];
      },
      sendOffsets: async offsets => {
        pendingOffsets = {
          group: resolveGroup(offsets.consumer),
          topics: offsets.topics,
        };
      },
      commit: async () => {
        pendingRecords.forEach(record => deliverRecord(record));
        pendingBatches.forEach(batch => deliverBatch(batch));
        if (pendingOffsets) {
          this.committedOffsets.push(pendingOffsets);
        }
      },
      abort: async () => {
        // Drop everything: an aborted transaction delivers nothing and commits
        // no offsets.
        pendingRecords.length = 0;
        pendingBatches.length = 0;
        pendingOffsets = undefined;
      },
    };
  }

  private createConsumer(config?: KafkaConsumerConfig): KafkaDriverConsumer {
    const consumer = {
      connect: async () => {},
      disconnect: async () => {},
      subscribe: async () => {},
      run: async () => {},
    };
    // Stash the resolved group so `sendOffsets(consumer)` can attribute offsets.
    groupOfConsumer.set(consumer, config?.groupId ?? 'default');
    return consumer;
  }

  private deliver(topic: string, messages: KafkaProducerMessage[]): void {
    for (const message of messages) {
      this.delivered.push({
        topic,
        key: decode(message.key),
        value: decode(message.value) ?? '',
      });
    }
  }
}

/**
 * Maps an in-memory consumer back to the group it was created with. Confluent's
 * `sendOffsets` takes the live consumer object (not a `consumerGroupId` string),
 * so the broker reads the group off the consumer the same way the real client
 * does internally.
 */
const groupOfConsumer = new WeakMap<KafkaDriverConsumer, string>();

function resolveGroup(consumer: KafkaDriverConsumer): string {
  return groupOfConsumer.get(consumer) ?? 'default';
}

function decode(
  value: KafkaProducerMessage['value'] | KafkaProducerMessage['key'],
): string | null {
  if (value === null || value === undefined) {
    return null;
  }
  return Buffer.isBuffer(value) ? value.toString('utf8') : value;
}

function metadata(topic: string, partition: number): KafkaRecordMetadata {
  return { topicName: topic, partition, errorCode: 0, offset: String(partition) };
}
