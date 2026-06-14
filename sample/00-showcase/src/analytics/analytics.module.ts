import { Module } from '@nestjs/common';
import { AnalyticsConsumer } from './analytics.consumer';

/**
 * The analytics feature module, registering the batch consumer that aggregates
 * order-revenue events per partition (milestone 5: batch consume + per-topic
 * concurrency).
 */
@Module({
  providers: [AnalyticsConsumer],
})
export class AnalyticsModule {}
