import { Module } from '@nestjs/common';
import { ActivityConsumer } from './activity.consumer';

/**
 * The activity feature module: one consumer subscribed by topic pattern to
 * every showcase event topic.
 */
@Module({
  providers: [ActivityConsumer],
})
export class ActivityModule {}
