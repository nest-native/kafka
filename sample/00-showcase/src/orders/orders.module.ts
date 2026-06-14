import { Module } from '@nestjs/common';
import {
  OrderErrorFilter,
  OrderValidationPipe,
  TenantGuard,
  TimingInterceptor,
} from '../common/enhancers';
import { OrderAuditService } from './order-audit.service';
import { OrdersConsumer } from './orders.consumer';
import { OrdersService } from './orders.service';

/**
 * The orders feature module: a producer service, a request-scoped consumer with
 * the full enhancer pipeline, and the enhancer providers themselves. The consumer
 * lives alongside its dependencies so Nest DI resolves them; the explorer
 * discovers `@KafkaConsumer` providers from any module.
 */
@Module({
  providers: [
    OrdersConsumer,
    OrdersService,
    OrderAuditService,
    TenantGuard,
    TimingInterceptor,
    OrderValidationPipe,
    OrderErrorFilter,
  ],
  exports: [OrdersService],
})
export class OrdersModule {}
