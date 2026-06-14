import {
  Inject,
  Injectable,
  OnApplicationShutdown,
  OnModuleInit,
} from '@nestjs/common';
import {
  KafkaDriverProducer,
  KafkaRecordMetadata,
  KafkaSendBatch,
  KafkaSendRecord,
  KafkaTransaction,
} from './driver';
import { KAFKA_PRODUCER } from './tokens';

/**
 * Work performed inside a Kafka transaction. The callback receives the
 * transaction handle; returning normally commits, throwing aborts.
 */
export type KafkaTransactionalWork<T> = (
  transaction: KafkaTransaction,
) => Promise<T> | T;

/**
 * High-level producer API for `@nest-native/kafka`.
 *
 * The service owns the lifecycle of a single underlying producer: it connects
 * on module init and disconnects on application shutdown, mirroring the graceful
 * shutdown contract the rest of the package follows. It exposes the three
 * publishing primitives promised by the public API — `send`, `sendBatch`, and
 * `transactional` — while keeping the raw producer reachable through
 * {@link InjectKafkaProducer} for advanced use.
 */
@Injectable()
export class KafkaProducerService
  implements OnModuleInit, OnApplicationShutdown
{
  private connected = false;

  constructor(
    @Inject(KAFKA_PRODUCER) private readonly producer: KafkaDriverProducer,
  ) {}

  /**
   * Connect the producer when the module initialises. Idempotent: a second call
   * is a no-op so manual {@link connect} calls compose cleanly.
   */
  async onModuleInit(): Promise<void> {
    await this.connect();
  }

  /**
   * Disconnect the producer during graceful shutdown.
   */
  async onApplicationShutdown(): Promise<void> {
    await this.disconnect();
  }

  /**
   * Connect the producer if it is not already connected.
   */
  async connect(): Promise<void> {
    if (this.connected) {
      return;
    }

    await this.producer.connect();
    this.connected = true;
  }

  /**
   * Disconnect the producer if it is currently connected.
   */
  async disconnect(): Promise<void> {
    if (!this.connected) {
      return;
    }

    await this.producer.disconnect();
    this.connected = false;
  }

  /**
   * Whether the producer connection is currently open.
   */
  isConnected(): boolean {
    return this.connected;
  }

  /**
   * Publish one or more messages to a single topic.
   */
  send(record: KafkaSendRecord): Promise<KafkaRecordMetadata[]> {
    return this.producer.send(record);
  }

  /**
   * Publish a batch of messages spanning one or more topics in a single call.
   */
  sendBatch(batch: KafkaSendBatch): Promise<KafkaRecordMetadata[]> {
    return this.producer.sendBatch(batch);
  }

  /**
   * Run `work` inside a Kafka transaction. The transaction commits when `work`
   * resolves and aborts when it rejects, re-throwing the original error so
   * callers see the failure.
   */
  async transactional<T>(work: KafkaTransactionalWork<T>): Promise<T> {
    const transaction = await this.producer.transaction();

    try {
      const result = await work(transaction);
      await transaction.commit();
      return result;
    } catch (error) {
      await transaction.abort();
      throw error;
    }
  }
}
