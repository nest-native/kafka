import { Injectable } from '@nestjs/common';
import { KafkaProducerService } from '@nest-native/kafka';
import { OrderPlaced } from './orders.consumer';

/**
 * Publishes orders.
 *
 * Migration note: in `@nestjs/microservices` you would inject a `ClientKafka`
 * and call `client.emit('orders.placed', order)`. Here you inject
 * `KafkaProducerService` and call `send` with an explicit topic/messages shape,
 * which maps directly onto Confluent's client.
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
}
