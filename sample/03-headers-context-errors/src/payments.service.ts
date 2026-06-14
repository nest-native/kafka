import { Injectable } from '@nestjs/common';
import { KafkaProducerService } from '@nest-native/kafka';
import { PAYMENTS_TOPIC, type PaymentEvent } from './payments.consumer';

/**
 * Publishes `payments.captured` events the {@link PaymentsConsumer} consumes,
 * attaching a neutral `x-tenant` header the consumer reads back through
 * `@KafkaHeaders('x-tenant')`.
 */
@Injectable()
export class PaymentsService {
  constructor(private readonly producer: KafkaProducerService) {}

  async capture(payment: PaymentEvent, tenant: string): Promise<void> {
    await this.producer.send({
      topic: PAYMENTS_TOPIC,
      messages: [
        {
          key: payment.id,
          headers: { 'x-tenant': tenant },
          value: JSON.stringify(payment),
        },
      ],
    });
  }
}
