# API Reference

The exported surface of `@nest-native/kafka`. Everything below is imported from
the package root.

## Module

| Export | Kind | Notes |
| --- | --- | --- |
| `KafkaModule` | class | `forRoot(options)`, `forRootAsync(options)`, `forFeature([HandlerClass])` — see [Module](module.md). |
| `KafkaModuleOptions` | interface | Options for `forRoot`. |
| `KafkaModuleAsyncOptions` | interface | Options for `forRootAsync`. |
| `KafkaConcurrencyOptions` | interface | `concurrency` and `maxInFlight`, shared by module / consumer / handler. |

## Consumer Decorators

| Export | Kind | Notes |
| --- | --- | --- |
| `KafkaConsumer` | decorator | Class-level: `@KafkaConsumer(topic?, options?)`. |
| `KafkaHandler` | decorator | Method-level: `@KafkaHandler(topic?, options?)`. |
| `KafkaConsumerOptions` | interface | `groupId` plus concurrency options. |
| `KafkaHandlerOptions` | interface | `batch`, `reply`, plus concurrency options. |

See [Consumers](consumers.md).

## Parameter Decorators

| Export | Kind | Notes |
| --- | --- | --- |
| `KafkaMessage` | decorator | Whole payload, or one property with `@KafkaMessage('prop')`. |
| `KafkaHeaders` | decorator | All headers, or one with `@KafkaHeaders('key')`. |
| `KafkaCtx` | decorator | The raw `KafkaContext`. |
| `KafkaBatch` | decorator | The raw `KafkaConsumerBatch` (batch handlers). |
| `KafkaContext` | class | Transport context: `getTopic()`, `getPartition()`, `getMessage()`, `getHeaders()`. |
| `KafkaBatchContext` | class | Batch transport context. |
| `KafkaMessageHeaders` | interface | The header map shape. |

See [Parameter Decorators](parameter-decorators.md).

## Producer

| Export | Kind | Notes |
| --- | --- | --- |
| `KafkaProducerService` | class | `send`, `sendBatch`, `transactional`. |
| `InjectKafkaProducer` | decorator | Inject the raw `KafkaDriverProducer`. |
| `KafkaTransaction` | interface | The transaction handle passed to `transactional`. |
| `KafkaSendRecord` | interface | A single `send` payload. |
| `KafkaSendBatch` | interface | A `sendBatch` payload. |
| `KafkaTransactionOffsets` | interface | The `sendOffsets` argument (Confluent shape). |

See [Producer](producer.md) and [Transactions](transactions.md).

## Error Mapping

| Export | Kind | Notes |
| --- | --- | --- |
| `KafkaErrorMapper` | type | `(error, context) => KafkaErrorBehavior`. |
| `KafkaErrorBehavior` | type | `'commit' \| 'retry'`. |
| `defaultKafkaErrorMapper` | const | Commits 4xx client errors, retries the rest. |
| `KafkaErrorContext` | type | `KafkaContext \| KafkaBatchContext`. |

See [Error Mapping](error-mapping.md).

## Request-Reply

Opt-in, and inert until `requestReply` is configured. See
[Request-Reply](request-reply.md).

| Export | Kind | Notes |
| --- | --- | --- |
| `KafkaRequestReplyService` | class | `request(record, options?)` — produce a request and await the correlated reply. |
| `KafkaRequestReplyOptions` | interface | The `requestReply` module block: `replyTopic` (required), `timeoutMs`, `readinessTimeoutMs`, `groupIdPrefix`, `headers`, `consumer`. |
| `KafkaRequestRecord` | interface | `{ topic, message }` — the request to produce. |
| `KafkaRequestOptions` | interface | Per-call `timeoutMs` and `signal`. |
| `KafkaReply` | interface | `{ value, headers, correlationId, topic, partition, offset? }`. |
| `KafkaRequestReplyHeaderKeys` | interface | The five header names; defaults interoperate with `@nestjs/microservices`. |
| `KafkaReplyTimeoutError` | class | No reply in time — the outcome is **unknown**. |
| `KafkaReplyRemoteError` | class | The remote handler failed and its error mapped to `'commit'`. |
| `KafkaReplyAbortedError` | class | The wait was cancelled by a signal or by shutdown. |
| `KafkaReplyDeliveryError` | class | Raised on the replying side when the reply could not be produced. |
| `DEFAULT_KAFKA_REQUEST_REPLY_HEADERS` | const | The default header key map. |
| `DEFAULT_REQUEST_TIMEOUT_MS` | const | `30000`. |
| `DEFAULT_READINESS_TIMEOUT_MS` | const | `10000`. |

## Driver

| Export | Kind | Notes |
| --- | --- | --- |
| `createConfluentDriver` | const | The default driver factory; lazily loads the Confluent client. |
| `KafkaClientDriver` | interface | The driver contract. |
| `KafkaDriverProducer` | interface | The producer the driver exposes. |
| `KafkaDriverConsumer` | interface | The consumer the driver exposes. |
| `KafkaDriverFactory` | type | `driverFactory` option shape. |

The driver is an advanced seam. Most applications never touch it directly.

## Testing

| Export | Kind | Notes |
| --- | --- | --- |
| `KafkaTestModule` | class | In-memory transport: `forRoot`, `forRootAsync`. |
| `InMemoryKafkaBroker` | class | The loopback broker: `emit`, `idle`, `getSent`, `getSentTo`. |
| `KAFKA_TEST_BROKER` | symbol | Injection token for the broker. |
| `InjectKafkaTestBroker` | decorator | Inject the broker. |
| `createMockKafkaProducer` | function | Recording producer mock. |
| `createMockTransaction` | function | Recording transaction mock. |

See [Testing](testing.md). Import the testing utilities from the
`@nest-native/kafka/testing` entrypoint — they are kept out of the package root
so test scaffolding never enters a consumer's production import surface.
