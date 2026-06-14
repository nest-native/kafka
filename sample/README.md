# Samples

The sample tree follows the same shape as the main quality strategy:

- `00-showcase`: the full integration baseline (arrives in a later milestone).
- `01-*` onward: focused samples that isolate one topic each.

## Commands

```bash
npm run ci:sample
npm run sample:focused
npm run test --workspace nest-native-kafka-sample-01-producer-basics
```

## Brokers

Samples run in memory by default so they need no Kafka broker and no native
`librdkafka` install. Set `KAFKA_BROKERS` (a comma-separated broker list) to run
a sample against a real cluster through Confluent's
`@confluentinc/kafka-javascript` client. Credentials must never be committed to
sample code, logs, or docs.
