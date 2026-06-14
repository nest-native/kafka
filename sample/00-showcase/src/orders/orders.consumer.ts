import {
  Logger,
  Scope,
  UseFilters,
  UseGuards,
  UseInterceptors,
  UsePipes,
} from '@nestjs/common';
import { Injectable } from '@nestjs/common';
import { KafkaConsumer, KafkaHandler, KafkaProducerService } from '@nest-native/kafka';
import {
  OrderErrorFilter,
  OrderValidationPipe,
  TenantGuard,
  TimingInterceptor,
} from '../common/enhancers';
import { MessageLog } from '../common/message-log.service';
import { OrderAuditService } from './order-audit.service';

export interface OrderPlaced {
  id: string;
  tenant: string;
  amount: number;
}

export const ORDERS_TOPIC = 'showcase.orders.placed';
export const NOTIFICATIONS_TOPIC = 'showcase.notifications';

/**
 * The orders consumer. It is request-scoped so it can depend on the
 * request-scoped {@link OrderAuditService}, and it carries the full enhancer
 * pipeline: a class-level guard and interceptor, plus a method-level pipe and
 * filter. After handling an order it publishes a derived notification event,
 * showing producer + consumer wired together in one feature.
 */
@Injectable({ scope: Scope.REQUEST })
@KafkaConsumer(ORDERS_TOPIC, { groupId: 'showcase-orders' })
@UseGuards(TenantGuard)
@UseInterceptors(TimingInterceptor)
export class OrdersConsumer {
  private readonly logger = new Logger(OrdersConsumer.name);

  constructor(
    private readonly log: MessageLog,
    private readonly audit: OrderAuditService,
    private readonly producer: KafkaProducerService,
  ) {}

  @KafkaHandler()
  @UsePipes(OrderValidationPipe)
  @UseFilters(OrderErrorFilter)
  async handle(order: OrderPlaced): Promise<void> {
    this.audit.audit();
    this.log.record('handledOrders', order.id);
    this.logger.log(`Handled order ${order.id} for tenant ${order.tenant}`);

    await this.producer.send({
      topic: NOTIFICATIONS_TOPIC,
      messages: [
        {
          key: order.id,
          // Header conventions stay neutral: the app picks its own key here.
          headers: { 'x-tenant': order.tenant },
          value: JSON.stringify({
            orderId: order.id,
            message: `Order ${order.id} confirmed`,
          }),
        },
      ],
    });
  }
}
