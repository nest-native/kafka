import { Injectable } from '@nestjs/common';
import { KafkaProducerService } from '@nest-native/kafka';
import type { OrderPlaced } from './orders.consumer';

/**
 * Publishes `orders.placed` events that the {@link OrdersConsumer} consumes,
 * wiring the producer and consumer halves of the sample together.
 */
@Injectable()
export class OrdersService {
  static readonly topic = 'orders.placed';

  constructor(private readonly producer: KafkaProducerService) {}

  async placeOrder(order: Partial<OrderPlaced>): Promise<void> {
    await this.producer.send({
      topic: OrdersService.topic,
      messages: [{ key: order.id ?? null, value: JSON.stringify(order) }],
    });
  }
}
