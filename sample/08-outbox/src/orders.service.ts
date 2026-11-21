import { Injectable } from '@nestjs/common';
import { Outbox } from '@nestjs/outbox';
import { SampleOutboxStore } from './sample-outbox.store';
import { ORDERS_PLACED } from './topics';

/** The event `OrdersService` publishes, and `BillingConsumer` charges for. */
export interface OrderPlaced {
  orderId: string;
  customerId: string;
  total: number;
}

/**
 * The producing side. Placing an order writes the order and its
 * `orders.placed` message in one transaction, so the message exists if and
 * only if the order does: there is no publish to forget after the commit, and
 * none to take back after a rollback. The relay publishes the message once the
 * transaction has committed.
 */
@Injectable()
export class OrdersService {
  /** Stands in for the orders table; a real app writes it through `tx` too. */
  readonly orders = new Map<string, OrderPlaced>();

  constructor(
    private readonly db: SampleOutboxStore,
    private readonly outbox: Outbox,
  ) {}

  /**
   * Place `order`. `check` runs inside the transaction, and throwing from it
   * rolls the transaction back: no order, no message.
   */
  async place(order: OrderPlaced, check: () => void = () => {}): Promise<string> {
    const message = await this.db.transaction(async tx => {
      const added = await this.outbox.add(tx, {
        topic: ORDERS_PLACED,
        // The ordering key: one order's messages are published one at a time,
        // in the order they were added, and land on one partition.
        key: order.orderId,
        headers: { 'x-customer-id': order.customerId },
        payload: order,
      });
      check();
      this.orders.set(order.orderId, order);
      return added;
    });
    // Committed: poll now instead of at the relay's next tick.
    this.outbox.notify();
    return message.id;
  }
}
