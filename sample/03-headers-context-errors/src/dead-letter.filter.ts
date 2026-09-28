import {
  ArgumentsHost,
  BadRequestException,
  Catch,
  ExceptionFilter,
  Injectable,
} from '@nestjs/common';
import {
  KafkaContext,
  KafkaProducerService,
  toDeadLetterMessage,
} from '@nest-native/kafka';

export const PAYMENTS_DEAD_LETTER_TOPIC = 'payments.captured.dlq';

/**
 * Dead-letters a payment the handler rejected as invalid, instead of letting
 * the default error mapper commit it silently.
 *
 * The record keeps the original key, value, and headers, plus Spring Kafka's
 * `kafka_dlt-*` headers saying where it came from and why it failed. The
 * transport awaits this filter: the poison message is committed only once its
 * dead letter is written, and a produce that fails leaves it to be retried.
 */
@Injectable()
@Catch(BadRequestException)
export class DeadLetterFilter implements ExceptionFilter {
  constructor(private readonly producer: KafkaProducerService) {}

  async catch(error: BadRequestException, host: ArgumentsHost): Promise<void> {
    const context = host.switchToRpc().getContext<KafkaContext>();
    await this.producer.send({
      topic: PAYMENTS_DEAD_LETTER_TOPIC,
      messages: [
        toDeadLetterMessage(context, error, { consumerGroup: 'payments-sample' }),
      ],
    });
  }
}
