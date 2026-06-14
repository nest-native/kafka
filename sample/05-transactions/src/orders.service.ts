import { Inject, Injectable } from '@nestjs/common';
import {
  KAFKA_CLIENT_DRIVER,
  KafkaClientDriver,
  KafkaDriverConsumer,
  KafkaProducerService,
} from '@nest-native/kafka';

export interface OrderPlaced {
  id: string;
  total: number;
}

export const ORDERS_TOPIC = 'orders.placed';
export const AUDIT_TOPIC = 'orders.audit';
export const PAYMENTS_TOPIC = 'payments.captured';
export const RECEIPTS_TOPIC = 'receipts.issued';

/**
 * Demonstrates the transactional producer helper (`KafkaProducerService.
 * transactional`). Every public method runs inside one atomic transaction: the
 * helper commits when the callback returns and aborts — delivering nothing —
 * when it throws.
 */
@Injectable()
export class OrdersService {
  constructor(
    private readonly producer: KafkaProducerService,
    @Inject(KAFKA_CLIENT_DRIVER) private readonly driver: KafkaClientDriver,
  ) {}

  /**
   * Publish an order plus its audit record atomically across two topics: either
   * both land or neither does.
   */
  async placeOrder(order: OrderPlaced): Promise<void> {
    await this.producer.transactional(async tx => {
      await tx.send({
        topic: ORDERS_TOPIC,
        messages: [{ key: order.id, value: JSON.stringify(order) }],
      });
      await tx.sendBatch({
        topicMessages: [
          {
            topic: AUDIT_TOPIC,
            messages: [{ key: order.id, value: `placed ${order.id}` }],
          },
        ],
      });
    });
  }

  /**
   * Reject an order inside a transaction. Throwing from the callback aborts the
   * transaction, so the partial writes already issued are discarded — nothing is
   * ever delivered. The original error is re-thrown to the caller.
   */
  async rejectOrder(order: OrderPlaced): Promise<void> {
    await this.producer.transactional(async tx => {
      await tx.send({
        topic: ORDERS_TOPIC,
        messages: [{ key: order.id, value: JSON.stringify(order) }],
      });
      // Some business rule fails after the write was staged.
      throw new Error(`order ${order.id} rejected`);
    });
  }

  /**
   * The consume-process-produce ("read-process-write") pattern that gives
   * exactly-once processing across a consume → produce step. The produced
   * receipt and the consumer offset for the captured payment commit atomically
   * via `sendOffsets`, so a crash can never double-issue a receipt nor skip one.
   */
  async issueReceipt(
    paymentId: string,
    sourcePartition: number,
    sourceOffset: string,
  ): Promise<KafkaDriverConsumer> {
    const consumer = this.driver.createConsumer({ groupId: 'payments-worker' });

    await this.producer.transactional(async tx => {
      await tx.send({
        topic: RECEIPTS_TOPIC,
        messages: [{ key: paymentId, value: `receipt for ${paymentId}` }],
      });
      // Commit the consumer's progress in the same transaction. Confluent's
      // client takes the live `consumer` object here (kafkajs took a
      // `consumerGroupId` string) — see the migration note in the README.
      await tx.sendOffsets({
        consumer,
        topics: [
          {
            topic: PAYMENTS_TOPIC,
            partitions: [
              {
                partition: sourcePartition,
                // Commit "next offset to read" = consumed offset + 1.
                offset: String(Number(sourceOffset) + 1),
              },
            ],
          },
        ],
      });
    });

    return consumer;
  }
}
