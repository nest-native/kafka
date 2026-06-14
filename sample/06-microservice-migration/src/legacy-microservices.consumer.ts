/**
 * The Kafka consumer **before** migrating — written against
 * `@nestjs/microservices`'s official Kafka transport.
 *
 * This file is a reference only; it is intentionally not wired into the
 * application (it depends on `@nestjs/microservices`'s `@EventPattern`/`@Payload`
 * and a `ClientKafka`). It sits beside the ported `OrdersConsumer` so you can
 * read the two side by side. The migration is a near-mechanical rename — see
 * `docs/migration-from-nestjs-microservices.md` for the full mapping.
 *
 * ```ts
 * import { Controller } from '@nestjs/common';
 * import { Ctx, EventPattern, KafkaContext, Payload } from '@nestjs/microservices';
 *
 * @Controller()
 * export class OrdersController {
 *   @EventPattern('orders.placed')
 *   handleOrderPlaced(
 *     @Payload() order: OrderPlaced,
 *     @Ctx() context: KafkaContext,
 *   ): void {
 *     // context.getTopic() / getPartition() / getMessage() are identical
 *     // in @nest-native/kafka's KafkaContext.
 *   }
 * }
 * ```
 *
 * Ported, this becomes the `@KafkaConsumer` / `@KafkaHandler` class in
 * `orders.consumer.ts`.
 */
export {};
