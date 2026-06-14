# Samples

The sample tree follows the same shape as the main quality strategy:

- `00-showcase` — the full integration baseline; grows with each milestone.
- `01-*` onward — focused samples that isolate one topic each.

Every sample runs **in memory by default**, so it needs no Kafka broker and no
native `librdkafka` install. The samples double as the CI proof that the public
API works end to end.

## Running The Samples

From the repository root:

```bash
# Validate every sample (typecheck + smoke test)
npm run ci:sample

# Just the focused samples
npm run sample:focused

# A single sample
npm run test --workspace nest-native-kafka-showcase
npm run test --workspace nest-native-kafka-sample-02-consumer-enhancers
```

## Running Against A Real Broker

Set `KAFKA_BROKERS` (a comma-separated broker list) to run a sample against a real
cluster through Confluent's `@confluentinc/kafka-javascript` client. Credentials
must never be committed to sample code, logs, or docs — the samples read brokers
from the environment only.

## Choosing A Sample

- New to the package? Start with `01-producer-basics`, then `02-consumer-enhancers`.
- Want the whole picture? Read `00-showcase`.
- Migrating an existing app? Read `06-microservice-migration` alongside the
  [Migration Guide](../migration.md).

See the [Sample Catalog](catalog.md) for the feature-by-feature index.
