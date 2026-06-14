import { Injectable, Logger } from '@nestjs/common';
import { KafkaConsumer, KafkaContext, KafkaHandler } from '@nest-native/kafka';
import { MessageLog } from '../common/message-log.service';
import { NOTIFICATIONS_TOPIC } from '../orders/orders.consumer';

interface NotificationEvent {
  orderId: string;
  message: string;
}

/**
 * A second feature's consumer, subscribed to the notification events the orders
 * consumer publishes. It shows a different consumer group and reading the raw
 * transport context through the second handler argument.
 */
@KafkaConsumer(NOTIFICATIONS_TOPIC, { groupId: 'showcase-notifications' })
export class NotificationsConsumer {
  private readonly logger = new Logger(NotificationsConsumer.name);

  constructor(private readonly log: MessageLog) {}

  @KafkaHandler()
  handle(event: NotificationEvent, context: KafkaContext): void {
    this.log.record('notifications', event.message);
    this.logger.log(
      `Notification on "${context.getTopic()}": ${event.message}`,
    );
  }
}
