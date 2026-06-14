import { Injectable, Logger } from '@nestjs/common';
import {
  KafkaConsumer,
  KafkaContext,
  KafkaCtx,
  KafkaHandler,
  KafkaHeaders,
  KafkaMessage,
} from '@nest-native/kafka';
import { MessageLog } from '../common/message-log.service';
import { NOTIFICATIONS_TOPIC } from '../orders/orders.consumer';

interface NotificationEvent {
  orderId: string;
  message: string;
}

/**
 * A second feature's consumer, subscribed to the notification events the orders
 * consumer publishes. It shows the milestone-4 parameter decorators: the parsed
 * payload via `@KafkaMessage()`, a single header by key via `@KafkaHeaders()`,
 * and the raw transport context via `@KafkaCtx()` — mirroring `@Payload()` /
 * `@Ctx()` from `@nestjs/microservices`.
 */
@KafkaConsumer(NOTIFICATIONS_TOPIC, { groupId: 'showcase-notifications' })
export class NotificationsConsumer {
  private readonly logger = new Logger(NotificationsConsumer.name);

  constructor(private readonly log: MessageLog) {}

  @KafkaHandler()
  handle(
    @KafkaMessage() event: NotificationEvent,
    @KafkaHeaders('x-tenant') tenant: string | Buffer | undefined,
    @KafkaCtx() context: KafkaContext,
  ): void {
    this.log.record('notifications', event.message);
    this.logger.log(
      `Notification on "${context.getTopic()}" for tenant "${String(
        tenant,
      )}": ${event.message}`,
    );
  }
}
