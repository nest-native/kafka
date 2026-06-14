import { Injectable, Logger } from '@nestjs/common';
import {
  KafkaBatch,
  KafkaConsumer,
  KafkaConsumerBatch,
  KafkaHandler,
  KafkaMessage,
} from '@nest-native/kafka';

export interface MetricEvent {
  meter: string;
  value: number;
}

export const METRICS_TOPIC = 'metrics.ingested';

/**
 * Records every batch the handler observed so the smoke test can assert batch
 * consumption (one invocation per fetched partition batch) and per-partition
 * concurrency (two partitions handled in the same run).
 */
@Injectable()
export class MetricsSink {
  readonly batches: {
    partition: number;
    count: number;
    sum: number;
  }[] = [];

  reset(): void {
    this.batches.length = 0;
  }
}

/**
 * A batch `@KafkaConsumer`. The handler runs once per fetched topic-partition
 * batch — not once per message — and reads:
 *
 * - `@KafkaMessage()` → the array of deserialized payloads in the batch.
 * - `@KafkaBatch()`   → the raw {@link KafkaConsumerBatch} (topic, partition,
 *   original messages with keys/headers/offsets).
 *
 * `concurrency: 2` lets two partitions process at the same time — the documented
 * opt-out of the official transport's sequential per-topic processing
 * (`nestjs/nest#12703`) — while ordering within each partition is preserved.
 */
@KafkaConsumer(METRICS_TOPIC, { groupId: 'metrics-aggregator', concurrency: 2 })
export class MetricsConsumer {
  private readonly logger = new Logger(MetricsConsumer.name);

  constructor(private readonly sink: MetricsSink) {}

  @KafkaHandler(undefined, { batch: true })
  aggregate(
    @KafkaMessage() metrics: MetricEvent[],
    @KafkaBatch() batch: KafkaConsumerBatch,
  ): void {
    const sum = metrics.reduce((total, metric) => total + metric.value, 0);
    this.sink.batches.push({
      partition: batch.partition,
      count: metrics.length,
      sum,
    });
    this.logger.log(
      `Aggregated ${metrics.length} metric(s) from partition ${batch.partition} (sum ${sum})`,
    );
  }
}
