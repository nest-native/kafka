import { NonRetryableMessageError } from '@nestjs/outbox';

/**
 * The produce failures a retry can never fix, by the numeric `code` on the error
 * the producer throws. Confluent's client puts librdkafka's error code there, on
 * both of its error shapes: the `LibrdKafkaError` that `produce()` throws before
 * a record is sent, and the `KafkaJSProtocolError` a failed delivery report
 * becomes. For an error the broker returned, that code is the Kafka protocol's
 * own, which is also what a kafkajs-based driver reports.
 *
 * Only failures of the record itself are listed. Everything else is retried by
 * the relay, including failures librdkafka calls permanent but that are not
 * about this record, or that a later attempt can outlive:
 * - `ERR__INVALID_DIFFERENT_RECORD` (-138): another record in the same batch was
 *   invalid, and this one failed with it.
 * - `ERR_RECORD_LIST_TOO_LARGE` (18): the batch, not the record, was over the
 *   segment size.
 * - `ERR__MSG_TIMED_OUT` (-192), `ERR_UNKNOWN_TOPIC_OR_PART` (3),
 *   `ERR__UNKNOWN_TOPIC` (-188), `ERR__UNKNOWN_PARTITION` (-190): the broker was
 *   unreachable, or the topic or partition may still be created.
 * - `ERR__KEY_SERIALIZATION` / `ERR__VALUE_SERIALIZATION` (-162 / -161): a
 *   serializer configured on the client can fail on a Schema Registry call.
 * - A fatal error (`ERR__FATAL`, -150): the producer is broken, not the record.
 */
const PERMANENT_FAILURES: ReadonlyMap<number, string> = new Map([
  // ERR_MSG_SIZE_TOO_LARGE: the record is over the producer's `message.max.bytes`,
  // and `produce()` refuses it before sending, or over the topic's
  // `max.message.bytes`, and the broker refuses it. librdkafka does not split a
  // batch the broker refused as too large: the whole batch fails with this code.
  [10, 'MSG_SIZE_TOO_LARGE'],
  // ERR_TOPIC_EXCEPTION ("Broker: Invalid topic"): not a legal topic name.
  // librdkafka marks such a topic as not existing at once, without waiting for
  // metadata to propagate.
  [17, 'TOPIC_EXCEPTION'],
  // ERR_TOPIC_AUTHORIZATION_FAILED: the client's principal may not write to the
  // topic. librdkafka fails the produce without retrying it.
  [29, 'TOPIC_AUTHORIZATION_FAILED'],
  // ERR_INVALID_RECORD: the broker failed to validate this record, for example a
  // record without a key on a compacted topic.
  [87, 'INVALID_RECORD'],
]);

/**
 * `error` wrapped in a `NonRetryableMessageError` when it is a produce failure a
 * retry can never fix, so the relay dead-letters the message at once instead of
 * spending its retry budget on it; `undefined` for any other failure.
 *
 * The original error is the `cause`, for `retry.retryIf` and the relay's events.
 * Its code and message are in the new error's message too, because the relay
 * records the message of what was thrown, not its cause.
 */
export function toPermanentFailure(
  error: unknown,
  topic: string,
): NonRetryableMessageError | undefined {
  if (!(error instanceof Error)) {
    return undefined;
  }
  const { code } = error as { code?: unknown };
  const failure =
    typeof code === 'number' ? PERMANENT_FAILURES.get(code) : undefined;
  if (failure === undefined) {
    return undefined;
  }
  return new NonRetryableMessageError(
    `Kafka refused the record for topic "${topic}" permanently ` +
      `(${failure}, code ${code}): ${error.message}`,
    { cause: error },
  );
}
