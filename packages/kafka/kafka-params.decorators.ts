import { createParamDecorator, ExecutionContext } from '@nestjs/common';
import { KafkaBatchContext, KafkaContext } from './kafka-context';
import { KafkaConsumerBatch, KafkaMessageHeaders } from './driver';

/**
 * Parameter decorators for `@KafkaHandler` methods.
 *
 * They mirror `@nestjs/microservices`'s `@Payload()` / `@Ctx()` ergonomics so a
 * handler porting from the official Kafka transport keeps the same shape, while
 * adding a first-class `@KafkaHeaders()` accessor. All three are built with
 * Nest's public {@link createParamDecorator}, so they participate in the full
 * enhancer pipeline: param-level pipes declared alongside them run exactly as
 * they do on an HTTP controller argument.
 *
 * The Kafka transport dispatches handlers under the `'rpc'` execution-context
 * type, so the decorators read the deserialized payload and the raw
 * {@link KafkaContext} through `ExecutionContext.switchToRpc()`.
 */

/**
 * Inject the deserialized message payload into a handler parameter.
 *
 * With no argument it resolves to the whole payload; passing a property name
 * resolves to that property of an object payload — mirroring `@Payload('id')`.
 *
 * @example
 * ```ts
 * @KafkaHandler('orders')
 * handle(@KafkaMessage() order: OrderPlaced) {}
 *
 * @KafkaHandler('orders')
 * handleId(@KafkaMessage('id') id: string) {}
 * ```
 *
 * @publicApi
 */
export const KafkaMessage = createParamDecorator(
  (property: string | undefined, ctx: ExecutionContext): unknown => {
    const payload = ctx.switchToRpc().getData<unknown>();
    if (property === undefined) {
      return payload;
    }
    return isRecord(payload) ? payload[property] : undefined;
  },
);

/**
 * Inject the message headers into a handler parameter.
 *
 * With no argument it resolves to the whole header map (empty when the message
 * carries none); passing a key resolves to that single header value.
 *
 * @example
 * ```ts
 * @KafkaHandler('orders')
 * handle(@KafkaHeaders() headers: KafkaMessageHeaders) {}
 *
 * @KafkaHandler('orders')
 * handleTrace(@KafkaHeaders('trace-id') traceId: string | Buffer) {}
 * ```
 *
 * @publicApi
 */
export const KafkaHeaders = createParamDecorator(
  (key: string | undefined, ctx: ExecutionContext): unknown => {
    const headers = ctx
      .switchToRpc()
      .getContext<KafkaContext>()
      .getHeaders();
    return key === undefined ? headers : (headers as KafkaMessageHeaders)[key];
  },
);

/**
 * Inject the raw {@link KafkaContext} (topic, partition, original message,
 * headers) into a handler parameter, mirroring `@Ctx()`.
 *
 * @example
 * ```ts
 * @KafkaHandler('orders')
 * handle(@KafkaCtx() context: KafkaContext) {
 *   context.getTopic();
 * }
 * ```
 *
 * @publicApi
 */
export const KafkaCtx = createParamDecorator(
  (_data: unknown, ctx: ExecutionContext): KafkaContext =>
    ctx.switchToRpc().getContext<KafkaContext>(),
);

/**
 * Inject the raw {@link KafkaConsumerBatch} (topic, partition, original messages)
 * into a `batch: true` handler parameter.
 *
 * Use it alongside `@KafkaMessage()` — which resolves to the array of
 * deserialized payloads on a batch handler — when you also need the per-message
 * keys, headers, or offsets the deserialized payloads drop.
 *
 * @example
 * ```ts
 * @KafkaHandler('metrics', { batch: true })
 * handle(
 *   @KafkaMessage() metrics: Metric[],
 *   @KafkaBatch() batch: KafkaConsumerBatch,
 * ) {
 *   batch.partition; // the partition the whole batch came from
 * }
 * ```
 *
 * @publicApi
 */
export const KafkaBatch = createParamDecorator(
  (_data: unknown, ctx: ExecutionContext): KafkaConsumerBatch =>
    ctx.switchToRpc().getContext<KafkaBatchContext>().getBatch(),
);

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null;
}
