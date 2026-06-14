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
| `KafkaHandlerOptions` | interface | `batch` plus concurrency options. |

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
| `InMemoryKafkaBroker` | class | The loopback broker: `emit`, `getSent`, `getSentTo`. |
| `KAFKA_TEST_BROKER` | symbol | Injection token for the broker. |
| `InjectKafkaTestBroker` | decorator | Inject the broker. |
| `createMockKafkaProducer` | function | Recording producer mock. |
| `createMockTransaction` | function | Recording transaction mock. |

See [Testing](testing.md). All testing utilities are also re-exported from the
package root.
