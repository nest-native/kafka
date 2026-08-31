import { Injectable } from '@nestjs/common';
import { KafkaConsumer, KafkaHandler, KafkaMessage } from '@nest-native/kafka';
import { MessageLog } from '../common/message-log.service';

/** The topic orders ask before they are published. */
export const INVENTORY_TOPIC = 'inventory.check';

/** What a caller asks for. */
export interface StockQuery {
  sku: string;
  quantity: number;
}

/** What the replying handler answers with. */
export interface StockAnswer {
  sku: string;
  available: boolean;
  onHand: number;
}

/**
 * The replying half of the request-reply showcase.
 *
 * `reply: true` is the whole opt-in: the handler's **post-enhancer return
 * value** becomes the reply body, addressed to whatever the request's headers
 * named. Everything else about the handler is unchanged — it is discovered the
 * same way, runs the same guard/interceptor/pipe/filter pipeline, and a
 * `@KafkaHandler` without the flag stays fire-and-forget.
 */
@Injectable()
@KafkaConsumer(INVENTORY_TOPIC, { groupId: 'showcase-inventory' })
export class InventoryConsumer {
  /** Deliberately small so the showcase can demonstrate both outcomes. */
  private readonly onHand = new Map<string, number>([
    ['widget', 12],
    ['gizmo', 0],
  ]);

  constructor(private readonly log: MessageLog) {}

  @KafkaHandler(INVENTORY_TOPIC, { reply: true })
  checkStock(@KafkaMessage() query: StockQuery): StockAnswer {
    const onHand = this.onHand.get(query.sku) ?? 0;
    const answer: StockAnswer = {
      sku: query.sku,
      available: onHand >= query.quantity,
      onHand,
    };
    this.log.record(
      'stockChecks',
      `stock check ${query.sku} x${query.quantity} -> ${
        answer.available ? 'available' : 'unavailable'
      } (${onHand} on hand)`,
    );
    return answer;
  }
}
