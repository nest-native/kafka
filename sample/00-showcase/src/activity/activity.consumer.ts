import { Injectable } from '@nestjs/common';
import {
  KafkaConsumer,
  KafkaContext,
  KafkaCtx,
  KafkaHandler,
} from '@nest-native/kafka';
import { MessageLog } from '../common/message-log.service';

/**
 * Every showcase event topic, by pattern — `orders`, `notifications`,
 * `analytics`, and any topic under them added later. It is anchored and names
 * its families on purpose: a looser `/^showcase\./` would also swallow
 * `showcase.replies`, the request-reply topic, and consume replies as events.
 * The pattern stays within POSIX extended syntax, which is how librdkafka
 * matches the subscription.
 */
export const SHOWCASE_EVENTS = /^showcase\.(orders|notifications|analytics)/;

/**
 * A pattern `@KafkaConsumer`: one activity feed over every matching topic,
 * including ones created after the application started, since the client
 * re-matches the pattern on every metadata refresh.
 */
@Injectable()
@KafkaConsumer(SHOWCASE_EVENTS, { groupId: 'showcase-activity' })
export class ActivityConsumer {
  constructor(private readonly log: MessageLog) {}

  @KafkaHandler()
  follow(@KafkaCtx() context: KafkaContext): void {
    this.log.recordActivity(context.getTopic());
  }
}
