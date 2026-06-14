# Why Native

`@nestjs/microservices`'s Kafka transport is built on
[`kafkajs`](https://github.com/tulios/kafkajs), which the community widely treats
as effectively unmaintained. Confluent staff offered their own client as the
replacement in [`nestjs/nest#13223`](https://github.com/nestjs/nest/issues/13223).
The official transport also carries unresolved correctness issues. This package
exists to keep the Nest ergonomics while standing on a maintained client and
closing those gaps.

## The Gaps This Package Closes

Each issue scenario has a regression test in the package suite.

| Issue | Symptom | What this package does |
| --- | --- | --- |
| [`#13223`](https://github.com/nestjs/nest/issues/13223) | kafkajs is unmaintained | Built on Confluent's `@confluentinc/kafka-javascript`, the client Confluent proposed as the replacement |
| [`#12703`](https://github.com/nestjs/nest/issues/12703) | Sequential per-topic processing | A `concurrency` option maps to `partitionsConsumedConcurrently`, with a documented default of `1` and an opt-out — see [Batch & Concurrency](batch-and-concurrency.md) |
| [`#12355`](https://github.com/nestjs/nest/issues/12355) | Rebalance hangs / lost progress | Batch consumers resolve each offset as it is processed, so a partition revoked mid-batch keeps the progress made — see [Batch & Concurrency](batch-and-concurrency.md) |
| [`#9679`](https://github.com/nestjs/nest/issues/9679) | Exceptions swallowed | Errors map to commit/retry behavior, with a configurable mapper — see [Error Mapping](error-mapping.md) |

## What "Native" Means Here

- The transport is a `CustomTransportStrategy` from `@nestjs/microservices`. It
  does not invent a new transport contract.
- Handlers are plain provider methods. Guards, interceptors, pipes, and filters
  apply at the global, controller, and method level — see [Consumers](consumers.md).
- Request-scoped providers and `REQUEST` injection resolve per consumed message.
- Offsets commit only after a handler returns successfully. In-flight messages
  complete or are explicitly aborted on shutdown — see
  [Graceful Shutdown](graceful-shutdown.md).

## What It Deliberately Stays Out Of

- Schema Registry integration (a follow-on package).
- Exactly-once helpers beyond what the Confluent client provides.
- A dead-letter-queue "framework" — the package provides primitives and documents
  the pattern.
- AsyncAPI generation, Kafka Streams, ksqlDB, and Kafka Connect.

The bar, restated from the project's constitution: feel like a first-class NestJS
transport, deliver on Confluent's officially supported client, and never hide
Kafka semantics. For boundaries over time, see the [Roadmap](roadmap.md).
