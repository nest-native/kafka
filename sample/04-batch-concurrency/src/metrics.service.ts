import { Injectable } from '@nestjs/common';
import { KafkaProducerService } from '@nest-native/kafka';
import { METRICS_TOPIC, type MetricEvent } from './metrics.consumer';

/**
 * Publishes a window of `metrics.ingested` events the {@link MetricsConsumer}
 * aggregates in batches. Each event names the partition it belongs to so the
 * sample can demonstrate per-partition concurrency: the in-memory broker groups
 * the window into one batch per partition.
 */
@Injectable()
export class MetricsService {
  constructor(private readonly producer: KafkaProducerService) {}

  async ingest(events: (MetricEvent & { partition: number })[]): Promise<void> {
    await this.producer.send({
      topic: METRICS_TOPIC,
      messages: events.map(event => ({
        key: event.meter,
        partition: event.partition,
        value: JSON.stringify({ meter: event.meter, value: event.value }),
      })),
    });
  }
}
