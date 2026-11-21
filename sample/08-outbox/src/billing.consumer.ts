import { Injectable, Logger } from '@nestjs/common';
import { KafkaConsumer, KafkaHandler, KafkaMessage } from '@nest-native/kafka';
import { type OutboxEnvelope, OutboxInbox } from '@nestjs/outbox';
import type { OrderPlaced } from './orders.service';
import { ORDERS_PLACED, TOPIC_PREFIX } from './topics';

/** What billing charged, once per order. */
@Injectable()
export class BillingLedger {
  readonly charges: OrderPlaced[] = [];

  charge(order: OrderPlaced): void {
    this.charges.push(order);
  }
}

/**
 * The consuming side, a plain `@KafkaHandler`.
 *
 * The record's value is the outbox envelope, so `@KafkaMessage()` hands over
 * `{ id, topic, key, headers, createdAt, payload }`. Kafka can deliver a record
 * twice (a relay retry whose first attempt landed after all, a rebalance before
 * the offset was committed), so the charge goes through `OutboxInbox`: it runs
 * once per envelope id, and a redelivery finds the id recorded and skips it.
 * Across instances that takes a shared inbox store (PostgreSQL, MySQL); with
 * side effects in the same database, `processInTransaction()` makes them
 * exactly-once.
 */
@Injectable()
@KafkaConsumer(`${TOPIC_PREFIX}${ORDERS_PLACED}`, { groupId: 'billing' })
export class BillingConsumer {
  private readonly logger = new Logger(BillingConsumer.name);

  constructor(
    private readonly inbox: OutboxInbox,
    private readonly ledger: BillingLedger,
  ) {}

  @KafkaHandler()
  async onOrderPlaced(
    @KafkaMessage() envelope: OutboxEnvelope<OrderPlaced>,
  ): Promise<void> {
    const { duplicate } = await this.inbox.process('billing', envelope.id, () =>
      this.ledger.charge(envelope.payload),
    );
    this.logger.log(
      `${duplicate ? 'Skipped a redelivery of' : 'Charged'} order ${envelope.payload.orderId}`,
    );
  }
}
