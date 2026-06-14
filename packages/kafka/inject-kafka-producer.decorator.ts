import { Inject } from '@nestjs/common';
import { KAFKA_PRODUCER } from './tokens';

/**
 * Inject the raw {@link KafkaDriverProducer} for direct, low-level access to the
 * Confluent producer.
 *
 * Most applications use {@link KafkaProducerService} instead; reach for this
 * decorator only when you need a Confluent producer feature the service does not
 * yet wrap.
 *
 * @example
 * ```ts
 * @Injectable()
 * class OutboxService {
 *   constructor(@InjectKafkaProducer() private readonly producer: KafkaDriverProducer) {}
 * }
 * ```
 */
export const InjectKafkaProducer = (): ParameterDecorator =>
  Inject(KAFKA_PRODUCER);
