import { Injectable, Logger } from '@nestjs/common';
import {
  KafkaConsumer,
  KafkaContext,
  KafkaCtx,
  KafkaHandler,
  KafkaMessage,
} from '@nest-native/kafka';

export interface OrderPlaced {
  id: string;
  total: number;
}

/**
 * An inbox the smoke test asserts against — the consumer records every handled
 * order here so a test can prove the ported handler ran.
 */
@Injectable()
export class OrdersInbox {
  readonly handled: { order: OrderPlaced; topic: string }[] = [];
}

/**
 * The Kafka consumer **after** migrating from `@nestjs/microservices`.
 *
 * Compare it with `legacy-microservices.consumer.ts`: the class is a
 * `@KafkaConsumer` instead of a `@Controller`, the method is `@KafkaHandler`
 * instead of `@EventPattern`, and the parameters use `@KafkaMessage()` /
 * `@KafkaCtx()` instead of `@Payload()` / `@Ctx()`. The handler body is
 * unchanged, and the full Nest enhancer pipeline still applies.
 */
@Injectable()
@KafkaConsumer('orders.placed', { groupId: 'orders-service' })
export class OrdersConsumer {
  private readonly logger = new Logger(OrdersConsumer.name);

  constructor(private readonly inbox: OrdersInbox) {}

  @KafkaHandler()
  handleOrderPlaced(
    @KafkaMessage() order: OrderPlaced,
    @KafkaCtx() context: KafkaContext,
  ): void {
    this.inbox.handled.push({ order, topic: context.getTopic() });
    this.logger.log(`Handled order ${order.id} on ${context.getTopic()}`);
  }
}
