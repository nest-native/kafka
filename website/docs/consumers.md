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

## Topic Patterns

A topic may be a `RegExp` — on the consumer or on a single handler — to consume
every topic it matches:

```ts
@KafkaConsumer(/^orders\.(placed|cancelled|refunded)$/, {groupId: 'order-audit'})
export class OrderAuditConsumer {
  @KafkaHandler()
  audit(order: OrderEvent, context: KafkaContext): void {
    // context.getTopic() says which one
  }
}
```

Confluent's client hands the pattern to `librdkafka`, which re-matches it on
every metadata refresh, so a matching topic created after the application
started is subscribed without a restart. The refresh runs every five minutes by
default (`topic.metadata.refresh.interval.ms`, settable on `client`).

The pattern is matched twice — by `librdkafka` for the subscription, and by this
package to route each record — so it must mean the same to both. That is why
bootstrap refuses:

- a pattern that does not start with `^`, or that carries flags — the only form
  the client accepts;
- JavaScript-only syntax, such as `\d`, `\w`, `(?:…)`, or lookarounds:
  `librdkafka` compiles the subscription as a POSIX extended regular
  expression, where `\d` is a literal `d`. Use `[0-9]`, `[A-Za-z0-9_]`, and
  plain groups;
- `reply: true` on a pattern — a replying handler names its request topic, or
  two repliers could match the same one.

A pattern matches every topic it matches, dead-letter and reply topics
included; anchor it tightly (`/^orders\.[a-z]+$/`, not `/^orders/`). A record
whose topic both a named handler and a pattern handler route runs both, the
named one first. Graceful shutdown pauses the named topics and every topic a
pattern has delivered so far.

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
