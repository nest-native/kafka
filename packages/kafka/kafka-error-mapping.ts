import { HttpException } from '@nestjs/common';
import { KafkaBatchContext, KafkaContext } from './kafka-context';

/**
 * The transport context handed to a {@link KafkaErrorMapper}: the per-message
 * {@link KafkaContext} for a single-message handler, or the
 * {@link KafkaBatchContext} for a batch handler. Both expose `getTopic()` /
 * `getPartition()` so a mapper can branch on the failed topic without caring
 * which consumption mode produced it.
 */
export type KafkaErrorContext = KafkaContext | KafkaBatchContext;

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
 *
 * It may be async: the transport awaits it before the record is committed or
 * handed back, so a dead-letter produce inside it finishes first. A mapper that
 * throws or rejects leaves the record to be retried — a dead-letter produce
 * that failed never loses the record.
 */
export type KafkaErrorMapper = (
  error: unknown,
  context: KafkaErrorContext,
) => KafkaErrorBehavior | Promise<KafkaErrorBehavior>;

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
 * Apply an {@link KafkaErrorMapper} to a failed message. Resolves when the error
 * maps to `'commit'` (the message is acknowledged); rejects with the original
 * error when it maps to `'retry'` so the caller can leave the offset
 * uncommitted and let the broker redeliver. A mapper that throws or rejects
 * rejects this too, with its own error — the record is retried.
 *
 * The mapper's result is awaited. Until 0.5.1 it was compared as returned, so
 * an async mapper's promise never equalled `'retry'`: every record it saw was
 * committed, before its dead-letter produce finished and even when that
 * produce failed.
 *
 * @internal
 */
export async function applyKafkaErrorBehavior(
  error: unknown,
  context: KafkaErrorContext,
  mapper: KafkaErrorMapper,
): Promise<void> {
  if ((await mapper(error, context)) === 'retry') {
    throw error;
  }
}
