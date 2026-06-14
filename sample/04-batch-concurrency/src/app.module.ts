import { Module } from '@nestjs/common';
import { KafkaModule } from '@nest-native/kafka';
import { InMemoryBroker } from './in-memory-broker';
import { resolveBrokers, resolveDriverFactory } from './kafka-driver';
import { MetricsConsumer, MetricsSink } from './metrics.consumer';
import { MetricsService } from './metrics.service';

/**
 * One in-memory broker shared by producer and consumer so an ingested window of
 * metrics loops straight back to the batch `@KafkaConsumer`, one batch per
 * partition.
 */
const broker = new InMemoryBroker();

export { broker };

@Module({
  imports: [
    KafkaModule.forRoot({
      clientId: 'sample-04-batch-concurrency',
      client: { brokers: resolveBrokers() },
      driverFactory: resolveDriverFactory(broker),
      // Module-wide default partition concurrency. A `@KafkaConsumer` or
      // `@KafkaHandler` may override it; `1` (the default here when omitted)
      // keeps strict per-partition ordering.
      concurrency: 1,
    }),
  ],
  providers: [MetricsConsumer, MetricsService, MetricsSink],
  exports: [MetricsService, MetricsSink],
})
export class AppModule {}
