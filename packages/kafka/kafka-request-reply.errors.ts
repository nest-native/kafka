import { KafkaReply } from './interfaces';

/**
 * No reply arrived for a request within its timeout.
 *
 * A timeout over Kafka means the outcome is **unknown**, never "it did not
 * happen": the request is still on the topic and may be processed after this
 * error is thrown. Retrying is the caller's decision, and a retry produces a
 * second request — the transport never manufactures duplicates on its own.
 *
 * @publicApi
 */
export class KafkaReplyTimeoutError extends Error {
  /** The topic the request was produced to. */
  readonly topic: string;
  readonly correlationId: string;
  readonly timeoutMs: number;

  constructor(topic: string, correlationId: string, timeoutMs: number) {
    super(
      `No reply for the Kafka request produced to "${topic}" within ` +
        `${timeoutMs}ms (correlation id "${correlationId}"). The outcome is ` +
        'unknown: the request may still be processed, so treat this as ' +
        '"undetermined", not as "it did not happen".',
    );
    this.name = 'KafkaReplyTimeoutError';
    this.topic = topic;
    this.correlationId = correlationId;
    this.timeoutMs = timeoutMs;
  }
}

/**
 * The reply arrived and is an error reply: the remote handler failed and its
 * error mapped to `'commit'`, so the failure is a final answer rather than work
 * awaiting redelivery.
 *
 * A `'retry'`-mapped remote failure produces no reply at all — the broker
 * redelivers and a later success can still resolve the request inside its
 * timeout window.
 *
 * @publicApi
 */
export class KafkaReplyRemoteError extends Error {
  /**
   * The decoded content of the error header: the parsed JSON when the remote
   * sent JSON (this package sends `{ name, message }`), the raw text otherwise.
   */
  readonly remote: unknown;
  /** The reply message that carried the error, headers included. */
  readonly reply: KafkaReply<unknown>;

  constructor(remote: unknown, reply: KafkaReply<unknown>, detail: string) {
    super(
      `The Kafka request (correlation id "${reply.correlationId}") was ` +
        `answered with an error reply: ${detail}`,
    );
    this.name = 'KafkaReplyRemoteError';
    this.remote = remote;
    this.reply = reply;
  }
}

/**
 * The wait for a reply was cancelled — by an {@link AbortSignal} the caller
 * aborted without a reason, or by application shutdown.
 *
 * Only the local wait is cancelled. There is no cross-process cancellation to
 * offer honestly, so the remote handler keeps running and its reply, if any,
 * is dropped as a late reply.
 *
 * @publicApi
 */
export class KafkaReplyAbortedError extends Error {
  constructor(
    message = 'The wait for a Kafka reply was aborted. The remote work is not ' +
      'cancelled — only this wait is.',
  ) {
    super(message);
    this.name = 'KafkaReplyAbortedError';
  }
}

/**
 * A replying handler produced its result but the reply could not be written to
 * the requester's reply topic.
 *
 * It is fed to the module's `errorMapper` like any other handler failure, so the
 * default is `'retry'`: from the requester's point of view an unsent reply is
 * unprocessed work, and redelivery is another chance to answer inside the
 * timeout window. Map it to `'commit'` when the reply address can never be
 * written to — an unroutable reply would otherwise retry forever.
 *
 * @publicApi
 */
export class KafkaReplyDeliveryError extends Error {
  /** The reply topic the request asked to be answered on. */
  readonly topic: string;
  readonly correlationId: string;

  constructor(topic: string, correlationId: string, cause: unknown) {
    super(
      `Failed to deliver the Kafka reply for correlation id ` +
        `"${correlationId}" to "${topic}". The requester will see a timeout ` +
        'unless a redelivery answers in time.',
    );
    this.name = 'KafkaReplyDeliveryError';
    this.topic = topic;
    this.correlationId = correlationId;
    (this as { cause?: unknown }).cause = cause;
  }
}
