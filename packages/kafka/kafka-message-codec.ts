import { KafkaIncomingMessage } from './kafka-context';

/**
 * Decode a consumed message value into a handler payload.
 *
 * Buffers and strings are JSON-parsed when they hold JSON, mirroring the default
 * deserializer of the official Kafka transport; a non-JSON value passes through
 * as the decoded string, and `null` (a tombstone) passes through unchanged. The
 * per-message and batch dispatch paths share this so both decode identically.
 *
 * @internal
 */
export function deserializeKafkaValue(
  value: KafkaIncomingMessage['value'],
): unknown {
  if (value === null) {
    return null;
  }
  const text = Buffer.isBuffer(value) ? value.toString('utf8') : value;
  try {
    return JSON.parse(text);
  } catch {
    return text;
  }
}
