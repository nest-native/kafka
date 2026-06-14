import { Injectable } from '@nestjs/common';
import { KafkaProducerService } from '@nest-native/kafka';
import { ORDERS_TOPIC, OrderPlaced } from './orders.consumer';

/**
 * The application service that publishes orders. It uses constructor injection
 * of {@link KafkaProducerService} — the producer half of the showcase.
 */
@Injectable()
export class OrdersService {
  constructor(private readonly producer: KafkaProducerService) {}

  async placeOrder(order: Partial<OrderPlaced>): Promise<void> {
    await this.producer.send({
      topic: ORDERS_TOPIC,
      messages: [{ key: order.id ?? null, value: JSON.stringify(order) }],
    });
  }
}
