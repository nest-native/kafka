import { Module } from '@nestjs/common';
import { KafkaModule } from '@nest-native/kafka';
import {
  BadRequestTraceFilter,
  NormalizeOrderPipe,
  PipelineTrace,
  TenantGuard,
  TimingInterceptor,
} from './enhancers';
import { OrdersConsumer, OrdersInbox } from './orders.consumer';
import { OrdersService } from './orders.service';
import { InMemoryBroker } from './in-memory-broker';
import { resolveBrokers, resolveDriverFactory } from './kafka-driver';

/**
 * The in-memory broker is shared between the producer (which publishes) and the
 * consumer (which subscribes), so a produced message loops straight back to the
 * `@KafkaConsumer` through the full enhancer pipeline.
 */
const broker = new InMemoryBroker();

@Module({
  imports: [
    KafkaModule.forRoot({
      clientId: 'sample-02-consumer-enhancers',
      client: { brokers: resolveBrokers() },
      driverFactory: resolveDriverFactory(broker),
    }),
  ],
  providers: [
    // The consumer and everything it depends on live in the same module so Nest
    // DI can resolve them. The explorer discovers `@KafkaConsumer` providers from
    // any module, so a dedicated `forFeature` is optional here.
    OrdersConsumer,
    OrdersService,
    OrdersInbox,
    // Enhancer classes are registered as providers so Nest DI can resolve them
    // for the `@UseGuards` / `@UseInterceptors` / `@UsePipes` / `@UseFilters`
    // declarations on the consumer, exactly as a Nest app registers them.
    PipelineTrace,
    TenantGuard,
    TimingInterceptor,
    NormalizeOrderPipe,
    BadRequestTraceFilter,
  ],
  exports: [OrdersService, OrdersInbox, PipelineTrace],
})
export class AppModule {}
