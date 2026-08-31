import { BadRequestException, Injectable, Logger } from '@nestjs/common';
import {
  KafkaConsumer,
  KafkaContext,
  KafkaCtx,
  KafkaHandler,
  KafkaMessage,
} from '@nest-native/kafka';

/** The request a caller sends to {@link TotalsConsumer.total}. */
export interface TotalQuery {
  customerId: string;
}

/** The reply it answers with. */
export interface TotalResult {
  customerId: string;
  total: number;
}

/**
 * An inbox the smoke test asserts against: every request the replying handlers
 * actually ran for.
 */
@Injectable()
export class TotalsInbox {
  readonly handled: { topic: string; customerId: string }[] = [];
}

/**
 * The replying side of request-reply.
 *
 * Two things are worth noticing, because both are the point of the design:
 *
 * 1. **`reply: true` is per handler.** `notifyPlaced` below sits on the same
 *    class with no flag and stays fire-and-forget, which is what
 *    `@KafkaHandler` is by default. Request-reply is never ambient.
 * 2. **This class needs no module configuration at all.** A replier learns
 *    where to answer from the request's own headers, which is exactly what
 *    lets it answer an un-migrated `@nestjs/microservices` `ClientKafka`
 *    (explicit reply partition and all) and this package's own `request()`
 *    through one code path.
 */
@Injectable()
@KafkaConsumer(undefined, { groupId: 'totals-service' })
export class TotalsConsumer {
  static readonly requestTopic = 'orders.total';
  static readonly rejectTopic = 'orders.total.rejected';
  static readonly eventTopic = 'orders.placed';

  private readonly logger = new Logger(TotalsConsumer.name);

  constructor(private readonly inbox: TotalsInbox) {}

  /**
   * The migration target for an `@MessagePattern('orders.total')` handler: the
   * return value becomes the reply, addressed by the request's headers. It is
   * the **post-enhancer** value, so interceptors and exception filters shape it
   * exactly as they would a `@MessagePattern` response.
   */
  @KafkaHandler(TotalsConsumer.requestTopic, { reply: true })
  total(
    @KafkaMessage() query: TotalQuery,
    @KafkaCtx() context: KafkaContext,
  ): TotalResult {
    this.inbox.handled.push({
      topic: context.getTopic(),
      customerId: query.customerId,
    });
    return { customerId: query.customerId, total: query.customerId.length * 10 };
  }

  /**
   * A failure the caller should learn about *now* rather than at timeout.
   *
   * The `errorMapper` decides: a 4xx maps to `'commit'`, which means "done", so
   * the failure is a final answer and goes back as an error reply. A
   * `'retry'`-mapped failure would send no reply at all — the broker
   * redelivers, and a later success can still answer inside the timeout window.
   */
  @KafkaHandler(TotalsConsumer.rejectTopic, { reply: true })
  rejected(@KafkaMessage() query: TotalQuery): never {
    this.inbox.handled.push({
      topic: TotalsConsumer.rejectTopic,
      customerId: query.customerId,
    });
    throw new BadRequestException(`unknown customer ${query.customerId}`);
  }

  /**
   * Unchanged by any of the above: no `reply: true`, so this is an ordinary
   * fire-and-forget event handler on the same consumer class.
   */
  @KafkaHandler(TotalsConsumer.eventTopic)
  notifyPlaced(@KafkaMessage() order: { id: string }): void {
    this.logger.log(`Order ${order.id} placed (fire-and-forget, no reply)`);
  }
}
