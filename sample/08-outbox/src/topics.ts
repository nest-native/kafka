import { KafkaOutboxTransport } from '@nest-native/kafka/outbox';

/** The topic `OrdersService` adds its outbox messages under. */
export const ORDERS_PLACED = 'orders.placed';

/**
 * A prefix on every Kafka topic this sample produces to, such as one per
 * environment sharing a cluster. Outbox messages keep their plain topic; the
 * transport adds the prefix when it produces.
 */
export const TOPIC_PREFIX = 'sample-08.';

/**
 * The transport class `OutboxModule.forRoot` instantiates: the default wire,
 * with the prefixed topic. `with()` is how the class form takes options;
 * `toRecord` returns only what differs from the default record.
 */
export const PrefixedKafkaOutboxTransport = KafkaOutboxTransport.with({
  toRecord: message => ({ topic: `${TOPIC_PREFIX}${message.topic}` }),
});
