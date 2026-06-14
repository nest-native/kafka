import { Injectable, Logger } from '@nestjs/common';
import {
  KafkaBatch,
  KafkaConsumer,
  KafkaConsumerBatch,
  KafkaHandler,
  KafkaMessage,
} from '@nest-native/kafka';
import { MessageLog } from '../common/message-log.service';

export const ANALYTICS_TOPIC = 'showcase.analytics.order-revenue';

interface RevenueEvent {
  orderId: string;
  amount: number;
}

/**
 * A batch `@KafkaConsumer` showing milestone 5: it aggregates order-revenue
 * events one batch per partition (`batch: true`) instead of one at a time, and
 * sets `concurrency: 2` so partitions are processed concurrently — the
 * documented opt-out of the official transport's sequential per-topic processing
 * (`nestjs/nest#12703`). Ordering within a partition is preserved.
 */
@KafkaConsumer(ANALYTICS_TOPIC, {
  groupId: 'showcase-analytics',
  concurrency: 2,
})
export class AnalyticsConsumer {
  private readonly logger = new Logger(AnalyticsConsumer.name);

  constructor(private readonly log: MessageLog) {}

  @KafkaHandler(undefined, { batch: true })
  aggregate(
    @KafkaMessage() events: RevenueEvent[],
    @KafkaBatch() batch: KafkaConsumerBatch,
  ): void {
    const revenue = events.reduce((total, event) => total + event.amount, 0);
    this.log.recordBatch(batch.partition, events.length);
    this.logger.log(
      `Aggregated ${events.length} revenue event(s) on partition ${batch.partition} (total ${revenue})`,
    );
  }
}
