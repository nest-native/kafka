import { BadRequestException, Injectable, Logger, UseFilters } from '@nestjs/common';
import {
  KafkaConsumer,
  KafkaContext,
  KafkaCtx,
  KafkaDeadLetterInfo,
  KafkaHandler,
  KafkaHeaders,
  KafkaMessage,
  readDeadLetterHeaders,
} from '@nest-native/kafka';
import { DeadLetterFilter, PAYMENTS_DEAD_LETTER_TOPIC } from './dead-letter.filter';

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

  /** Rejected payments, as they arrived on the dead-letter topic. */
  readonly deadLetters: {
    payment: PaymentEvent;
    tenant: string | Buffer | undefined;
    info: KafkaDeadLetterInfo | undefined;
  }[] = [];

  reset(): void {
    this.handled.length = 0;
    this.deadLetters.length = 0;
  }
}

export const PAYMENTS_TOPIC = 'payments.captured';

/**
 * A `@KafkaConsumer` showing the milestone-4 parameter decorators and error
 * mapping. The handler reads the payload, a single header by key, and the raw
 * transport context through separate decorated parameters, then rejects a
 * negative amount with a `BadRequestException`. The {@link DeadLetterFilter}
 * writes that poison message to the dead-letter topic before it is committed,
 * so it is neither redelivered forever nor lost.
 */
@KafkaConsumer(PAYMENTS_TOPIC, { groupId: 'payments-sample' })
export class PaymentsConsumer {
  private readonly logger = new Logger(PaymentsConsumer.name);

  constructor(private readonly inbox: PaymentsInbox) {}

  @UseFilters(DeadLetterFilter)
  @KafkaHandler()
  handle(
    @KafkaMessage() payment: PaymentEvent,
    @KafkaHeaders('x-tenant') tenant: string | Buffer | undefined,
    @KafkaCtx() context: KafkaContext,
  ): void {
    if (payment.amount < 0) {
      // Invalid, so it can never succeed: the filter dead-letters it, and the
      // message is then committed instead of being redelivered forever.
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

/**
 * Reads the dead-letter topic back, decoding the `kafka_dlt-*` headers with
 * `readDeadLetterHeaders` — what an operator's replay or alerting job would do.
 */
@KafkaConsumer(PAYMENTS_DEAD_LETTER_TOPIC, { groupId: 'payments-dead-letters' })
export class PaymentsDeadLetters {
  constructor(private readonly inbox: PaymentsInbox) {}

  @KafkaHandler()
  record(
    @KafkaMessage() payment: PaymentEvent,
    @KafkaHeaders('x-tenant') tenant: string | Buffer | undefined,
    @KafkaCtx() context: KafkaContext,
  ): void {
    this.inbox.deadLetters.push({
      payment,
      tenant,
      info: readDeadLetterHeaders(context.getHeaders()),
    });
  }
}
