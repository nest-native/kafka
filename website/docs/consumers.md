# Consumers

Consumers are plain Nest providers. Mark the class with `@KafkaConsumer` and its
methods with `@KafkaHandler`. The methods run through the full Nest enhancer
pipeline — exactly as they do for an HTTP controller or a `@nestjs/microservices`
handler.

## Declaring A Consumer

```ts
import {Injectable} from '@nestjs/common';
import {KafkaConsumer, KafkaContext, KafkaHandler} from '@nest-native/kafka';

@Injectable()
@KafkaConsumer('orders.placed', {groupId: 'orders-service'})
export class OrdersConsumer {
  @KafkaHandler()
  handle(order: OrderPlaced, context: KafkaContext): void {
    console.log(`order on ${context.getTopic()}`, order);
  }
}
```

- `@KafkaConsumer(topic?, options?)` — class level. `options.groupId` sets the
  consumer group; `options.concurrency` and `options.maxInFlight` set defaults for
  its handlers.
- `@KafkaHandler(topic?, options?)` — method level. When `topic` is omitted the
  handler inherits the consumer's topic.

The parsed payload is the first positional argument and the raw `KafkaContext` is
the second. For named parameters instead, see
[Parameter Decorators](parameter-decorators.md).

## Registering Consumers

Register the consumer (and any guard / interceptor / pipe / filter classes it
uses) as providers, then list it in `KafkaModule.forFeature([OrdersConsumer])` or
directly in a module's `providers`. Consumers in the same consumer group share a
single Confluent consumer so partitions balance across instances.

## The Enhancer Pipeline

`@UseGuards`, `@UseInterceptors`, `@UsePipes`, and `@UseFilters` work on handler
methods, at the global, controller, and method level — and this is
non-negotiable per the project's constitution:

```ts
import {Injectable, UseFilters, UseGuards, UseInterceptors, UsePipes} from '@nestjs/common';
import {KafkaConsumer, KafkaHandler, KafkaMessage} from '@nest-native/kafka';

@Injectable()
@KafkaConsumer('orders.placed', {groupId: 'orders-service'})
@UseGuards(TenantGuard)
export class OrdersConsumer {
  @KafkaHandler()
  @UseInterceptors(MetricsInterceptor)
  @UsePipes(new ValidationPipe({transform: true}))
  @UseFilters(OrdersExceptionFilter)
  handle(@KafkaMessage() order: OrderDto): void {
    // runs after guards, interceptors, and pipes; the filter wraps it
  }
}
```

- **Guards** decide whether the handler runs at all. Returning `false` (or
  throwing) skips the handler; the error then flows through
  [error mapping](error-mapping.md) like any other.
- **Pipes** transform and validate the payload, including `ValidationPipe` with
  `class-validator` DTOs and Zod pipes.
- **Interceptors** wrap execution to add metrics, logging, or timeouts.
- **Filters** catch thrown errors; an error a filter handles never reaches the
  error mapper.

## Validation

Both validation worlds are supported, app-owned:

- `class-validator` + DTOs through `ValidationPipe`, the default for teams coming
  from `@nestjs/microservices`.
- Zod, through a Zod validation pipe, for schema-derived types.

Neither validator is a runtime dependency of this package; install whichever your
app uses.

## Request Scope

Request-scoped consumers resolve a fresh instance per consumed message, and
`REQUEST` injection works. Use this for per-message context such as a tenant
resolved by a guard.

## Next

- [Parameter Decorators](parameter-decorators.md): `@KafkaMessage`, `@KafkaHeaders`, `@KafkaCtx`, `@KafkaBatch`.
- [Error Mapping](error-mapping.md): commit vs. retry when a handler throws.
- [Batch & Concurrency](batch-and-concurrency.md): batch handlers and per-topic concurrency.
