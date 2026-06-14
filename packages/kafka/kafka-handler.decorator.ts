import { KAFKA_HANDLER_METADATA } from './constants';
import { KafkaHandlerMetadata, KafkaHandlerOptions } from './interfaces';

/**
 * Mark a method as a Kafka handler.
 *
 * The decorated method is invoked for every message delivered on its topic,
 * after the full Nest enhancer pipeline (guards → interceptors → pipes) has run
 * and with exception filters applied around the call — exactly mirroring how
 * `@MessagePattern` / `@EventPattern` behave in `@nestjs/microservices`.
 *
 * When `topic` is omitted the handler inherits the topic declared on its owning
 * {@link KafkaConsumer}. A handler with neither its own topic nor an inherited
 * one is a configuration error surfaced at bootstrap.
 *
 * @param topic - Optional topic (or pattern) this method consumes.
 * @param options - Optional per-handler consumer-group override.
 *
 * @publicApi
 */
export function KafkaHandler(
  topic?: string,
  options: KafkaHandlerOptions = {},
): MethodDecorator {
  const metadata: KafkaHandlerMetadata = { topic, options };

  return (
    target: object,
    key: string | symbol,
    descriptor: PropertyDescriptor,
  ) => {
    Reflect.defineMetadata(KAFKA_HANDLER_METADATA, metadata, descriptor.value);
    return descriptor;
  };
}
