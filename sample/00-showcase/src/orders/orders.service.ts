import { Injectable, Logger } from '@nestjs/common';
import {
  KafkaProducerService,
  KafkaReplyTimeoutError,
  KafkaRequestReplyService,
} from '@nest-native/kafka';
import {
  INVENTORY_TOPIC,
  StockAnswer,
  StockQuery,
} from '../inventory/inventory.consumer';
import { ORDERS_TOPIC, OrderPlaced } from './orders.consumer';

/**
 * The application service that publishes orders. It uses constructor injection
 * of {@link KafkaProducerService} — the producer half of the showcase — and of
 * {@link KafkaRequestReplyService} for the one step that genuinely needs an
 * answer before it can proceed.
 *
 * The order is published through the transactional helper (milestone 6): the
 * write commits when the callback returns and aborts if it throws, so an order
 * is delivered atomically.
 */
@Injectable()
export class OrdersService {
  private readonly logger = new Logger(OrdersService.name);

  constructor(
    private readonly producer: KafkaProducerService,
    private readonly requests: KafkaRequestReplyService,
  ) {}

  /**
   * Ask inventory whether the order can be fulfilled, and publish it only if it
   * can.
   *
   * This is the shape request-reply is *for*: the caller cannot continue until
   * it has the answer. Everything else in this showcase is an event — published
   * and forgotten — which is the default `@KafkaHandler` behaviour and the right
   * one for most Kafka work.
   */
  async placeOrder(order: Partial<OrderPlaced>): Promise<boolean> {
    const sku = order.sku ?? 'widget';
    const quantity = order.quantity ?? 1;

    const stock = await this.checkStock({ sku, quantity });
    if (!stock) {
      return false;
    }
    if (!stock.available) {
      this.logger.warn(
        `Order ${order.id} not placed: ${sku} unavailable (${stock.onHand} on hand).`,
      );
      return false;
    }

    await this.producer.transactional(async tx => {
      await tx.send({
        topic: ORDERS_TOPIC,
        messages: [{ key: order.id ?? null, value: JSON.stringify(order) }],
      });
    });
    return true;
  }

  /**
   * A timeout means the outcome is *unknown*, not that the check failed — so the
   * order is held rather than published or rejected outright. Deciding what to
   * do with an unknown is the caller's job; the transport refuses to guess, and
   * deliberately does not retry on your behalf.
   */
  private async checkStock(query: StockQuery): Promise<StockAnswer | undefined> {
    try {
      const reply = await this.requests.request<StockAnswer>({
        topic: INVENTORY_TOPIC,
        message: { value: JSON.stringify(query) },
      });
      return reply.value;
    } catch (error) {
      if (error instanceof KafkaReplyTimeoutError) {
        this.logger.warn(
          `Stock check for ${query.sku} timed out — outcome unknown, holding the order.`,
        );
        return undefined;
      }
      throw error;
    }
  }
}
