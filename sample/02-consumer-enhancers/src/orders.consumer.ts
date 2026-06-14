import {
  Injectable,
  Logger,
  UseFilters,
  UseGuards,
  UseInterceptors,
  UsePipes,
} from '@nestjs/common';
import { KafkaConsumer, KafkaHandler } from '@nest-native/kafka';
import {
  BadRequestTraceFilter,
  NormalizeOrderPipe,
  PipelineTrace,
  TenantGuard,
  TimingInterceptor,
} from './enhancers';

export interface OrderPlaced {
  id: string;
  tenant: string;
}

/**
 * Collects the orders that survived the full enhancer pipeline so the smoke test
 * can assert which messages actually reached the handler body.
 */
@Injectable()
export class OrdersInbox {
  readonly handled: OrderPlaced[] = [];
}

/**
 * A `@KafkaConsumer` showing class- and method-level enhancers on the Kafka
 * transport. Guards and interceptors are declared on the class; the pipe and
 * filter are declared on the method. They all run for every consumed message,
 * exactly as they do for an HTTP controller or a `@nestjs/microservices`
 * handler.
 */
@KafkaConsumer('orders.placed', { groupId: 'orders-consumer-sample' })
@UseGuards(TenantGuard)
@UseInterceptors(TimingInterceptor)
export class OrdersConsumer {
  private readonly logger = new Logger(OrdersConsumer.name);

  constructor(
    private readonly inbox: OrdersInbox,
    private readonly trace: PipelineTrace,
  ) {}

  @KafkaHandler()
  @UsePipes(NormalizeOrderPipe)
  @UseFilters(BadRequestTraceFilter)
  handle(order: OrderPlaced): void {
    this.trace.record(`handler:${order.id}`);
    this.logger.log(`Handling order ${order.id} for tenant ${order.tenant}`);
    this.inbox.handled.push(order);
  }
}
