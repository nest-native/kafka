import { Injectable } from '@nestjs/common';
import { KafkaProducerService } from '@nest-native/kafka';

export interface OrderPlaced {
  id: string;
  total: number;
}

/**
 * A feature service that publishes domain events through the injected
 * {@link KafkaProducerService}. It shows the two single-call primitives this
 * milestone ships: `send` (one topic) and `sendBatch` (many topics at once).
 */
@Injectable()
export class OrdersService {
  static readonly topic = 'orders.placed';

  constructor(private readonly producer: KafkaProducerService) {}

  async placeOrder(order: OrderPlaced): Promise<void> {
    await this.producer.send({
      topic: OrdersService.topic,
      messages: [{ key: order.id, value: JSON.stringify(order) }],
    });
  }

  async placeOrders(orders: OrderPlaced[]): Promise<void> {
    await this.producer.sendBatch({
      topicMessages: [
        {
          topic: OrdersService.topic,
          messages: orders.map(order => ({
            key: order.id,
            value: JSON.stringify(order),
          })),
        },
      ],
    });
  }
}
