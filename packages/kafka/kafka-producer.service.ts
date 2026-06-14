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
   *
   * The callback receives the transaction handle, so it can `send`/`sendBatch`
   * its produced messages and `sendOffsets` to commit consumer progress
   * atomically with them (the consume-process-produce pattern). If the abort
   * itself fails while unwinding a failed `work`, the original error still
   * surfaces — the abort failure is attached as its `cause` so neither is lost.
   */
  async transactional<T>(work: KafkaTransactionalWork<T>): Promise<T> {
    const transaction = await this.producer.transaction();

    let result: T;
    try {
      result = await work(transaction);
    } catch (error) {
      await this.abortQuietly(transaction, error);
      throw error;
    }

    await transaction.commit();
    return result;
  }

  /**
   * Abort `transaction` while preserving `cause` as the error to throw. A
   * failure during abort is recorded on `cause` (when it is an `Error`) instead
   * of replacing it, so the root cause of the failed work is never masked by a
   * secondary abort error.
   */
  private async abortQuietly(
    transaction: KafkaTransaction,
    cause: unknown,
  ): Promise<void> {
    try {
      await transaction.abort();
    } catch (abortError) {
      if (cause instanceof Error) {
        (cause as { cause?: unknown }).cause ??= abortError;
      }
    }
  }
}
