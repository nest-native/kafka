import { HttpException } from '@nestjs/common';
import { KafkaContext } from './kafka-context';

/**
 * What the transport does with a message after a handler (or its enhancer
 * pipeline) throws and no exception filter handled the error.
 *
 * - `'commit'`  — treat the message as consumed and advance the offset. Use for
 *   non-retryable failures (validation errors, malformed payloads) so a poison
 *   message does not block the partition forever.
 * - `'retry'`   — do not advance the offset; surface the error so the driver
 *   redelivers the message (Kafka redelivers from the last committed offset).
 *   Use for transient failures (a downstream timeout) that a later attempt may
 *   recover from.
 */
export type KafkaErrorBehavior = 'commit' | 'retry';

/**
 * Decides the {@link KafkaErrorBehavior} for a failed message. Supply your own
 * through {@link KafkaModuleOptions.errorMapper} to override the
 * {@link defaultKafkaErrorMapper} — for example to route a specific error to a
 * dead-letter topic before committing.
 */
export type KafkaErrorMapper = (
  error: unknown,
  context: KafkaContext,
) => KafkaErrorBehavior;

/**
 * The default mapping from a thrown error to consumer behaviour, addressing the
 * "exception swallowing" gap (`nestjs/nest#9679`) the official transport has:
 * errors are never silently dropped — they are classified and surfaced.
 *
 * - A 4xx {@link HttpException} (e.g. `BadRequestException`) is a client/payload
 *   error the same message will keep failing on, so it commits (no infinite
 *   redelivery of a poison message).
 * - Any other error — a 5xx {@link HttpException}, an `RpcException`, or an
 *   arbitrary thrown value — is assumed transient and retried (the offset is not
 *   committed, so the broker redelivers).
 */
export const defaultKafkaErrorMapper: KafkaErrorMapper = error => {
  if (error instanceof HttpException) {
    const status = error.getStatus();
    return status >= 400 && status < 500 ? 'commit' : 'retry';
  }
  return 'retry';
};

/**
 * Apply an {@link KafkaErrorMapper} to a failed message. Returns when the error
 * maps to `'commit'` (the message is acknowledged); re-throws the original error
 * when it maps to `'retry'` so the caller can leave the offset uncommitted and
 * let the broker redeliver.
 *
 * @internal
 */
export function applyKafkaErrorBehavior(
  error: unknown,
  context: KafkaContext,
  mapper: KafkaErrorMapper,
): void {
  if (mapper(error, context) === 'retry') {
    throw error;
  }
}
