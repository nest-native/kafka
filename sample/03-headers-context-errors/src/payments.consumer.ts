import { BadRequestException, Injectable, Logger } from '@nestjs/common';
import {
  KafkaConsumer,
  KafkaContext,
  KafkaCtx,
  KafkaHandler,
  KafkaHeaders,
  KafkaMessage,
} from '@nest-native/kafka';

export interface PaymentEvent {
  id: string;
  amount: number;
}

/**
 * Records what the handler observed so the smoke test can assert the parameter
 * decorators resolved the payload, headers, and context independently.
 */
@Injectable()
export class PaymentsInbox {
  readonly handled: {
    payment: PaymentEvent;
    tenant: string | Buffer | undefined;
    topic: string;
    partition: number;
  }[] = [];

  reset(): void {
    this.handled.length = 0;
  }
}

export const PAYMENTS_TOPIC = 'payments.captured';

/**
 * A `@KafkaConsumer` showing the milestone-4 parameter decorators and error
 * mapping. The handler reads the payload, a single header by key, and the raw
 * transport context through separate decorated parameters, then rejects a
 * negative amount with a `BadRequestException` — a 4xx the default error mapper
 * commits (no infinite redelivery of a poison message).
 */
@KafkaConsumer(PAYMENTS_TOPIC, { groupId: 'payments-sample' })
export class PaymentsConsumer {
  private readonly logger = new Logger(PaymentsConsumer.name);

  constructor(private readonly inbox: PaymentsInbox) {}

  @KafkaHandler()
  handle(
    @KafkaMessage() payment: PaymentEvent,
    @KafkaHeaders('x-tenant') tenant: string | Buffer | undefined,
    @KafkaCtx() context: KafkaContext,
  ): void {
    if (payment.amount < 0) {
      // 4xx → the default mapper commits, so this poison message is acknowledged
      // instead of being redelivered forever.
      throw new BadRequestException(`negative amount for ${payment.id}`);
    }

    this.inbox.handled.push({
      payment,
      tenant,
      topic: context.getTopic(),
      partition: context.getPartition(),
    });
    this.logger.log(
      `Captured ${payment.id} (${payment.amount}) for tenant "${String(
        tenant,
      )}"`,
    );
  }
}
