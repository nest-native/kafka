import { Logger } from '@nestjs/common';
import { KafkaRequestReplyHeaderKeys } from './interfaces';
import { KafkaContext } from './kafka-context';
import { KafkaReplyDeliveryError } from './kafka-request-reply.errors';
import {
  KafkaReplyAddress,
  KafkaReplyOutcome,
  KafkaReplySender,
  buildReplyMessage,
  resolveReplyAddress,
} from './kafka-request-reply.protocol';

/**
 * The replying half of request-reply: turns a `reply: true` handler's outcome
 * into a message on whatever reply address the request advertised.
 *
 * The address comes from the request's own headers, so a replier needs no
 * configuration at all — which is what lets a handler migrated from
 * `@MessagePattern` answer an un-migrated `ClientKafka` (explicit reply
 * partition and all) and this package's own `request()` (no partition
 * targeting) with the same code path.
 *
 * @internal
 */
export class KafkaReplyPublisher {
  private readonly logger = new Logger(KafkaReplyPublisher.name);

  constructor(
    private readonly producer: KafkaReplySender,
    private readonly keys: KafkaRequestReplyHeaderKeys,
  ) {}

  /**
   * Produce the reply for one consumed request, or skip the step when the
   * message carries no usable reply address.
   *
   * @throws KafkaReplyDeliveryError when the reply itself could not be produced.
   * The dispatcher feeds that to the module's `errorMapper` like any other
   * handler failure, so the default is redelivery — from the requester's point
   * of view an unsent reply is unprocessed work.
   */
  async publish(
    context: KafkaContext,
    outcome: KafkaReplyOutcome,
  ): Promise<void> {
    const target = resolveReplyAddress(context.getHeaders(), this.keys);

    if (target.status === 'none') {
      // Legitimate traffic: a replayed request nobody is waiting on, or an event
      // produced onto a topic that also serves requests. The handler already
      // ran; only the reply step is skipped.
      this.logger.debug(
        `Message on "${context.getTopic()}" carries no reply address; ` +
          'the handler ran and the reply step was skipped.',
      );
      return;
    }

    if (target.status === 'undeliverable') {
      this.logger.warn(target.detail);
      return;
    }

    await this.send(target.address, outcome);
  }

  private async send(
    address: KafkaReplyAddress,
    outcome: KafkaReplyOutcome,
  ): Promise<void> {
    try {
      await this.producer.send({
        topic: address.topic,
        messages: [buildReplyMessage(address, this.keys, outcome)],
      });
    } catch (cause) {
      throw new KafkaReplyDeliveryError(
        address.topic,
        address.correlationId,
        cause,
      );
    }
  }
}
