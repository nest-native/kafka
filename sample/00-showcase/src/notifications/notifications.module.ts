import { Module } from '@nestjs/common';
import { NotificationsConsumer } from './notifications.consumer';

/**
 * The notifications feature module, registering its own consumer as a provider.
 */
@Module({
  providers: [NotificationsConsumer],
})
export class NotificationsModule {}
