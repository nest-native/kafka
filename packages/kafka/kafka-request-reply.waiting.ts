import { KafkaReplyAbortedError } from './kafka-request-reply.errors';

/**
 * The bounded wait every request-reply call is built on.
 *
 * A wait ends three ways — the work settles, a deadline passes, or an
 * {@link AbortSignal} fires — and all three have to release *both* the timer and
 * the abort listener. Composing two independent helpers cannot do that: an abort
 * would leave the deadline's timer running (holding the event loop for the whole
 * timeout on a wait nobody is watching any more), and a deadline would leave a
 * listener attached to a signal the caller may reuse for many requests. One
 * function owning both is the only shape without a leak.
 *
 * @internal
 */
export function waitForReply<T>(
  work: Promise<T>,
  timeoutMs: number,
  onTimeout: () => Error,
  signal?: AbortSignal,
): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    let detachAbort = (): void => {};
    const timer = setTimeout(() => {
      release();
      reject(onTimeout());
    }, timeoutMs);
    // The timer is deliberately not `unref`ed: an unref'ed timer would let the
    // process exit with the caller's promise never settling at all, which is a
    // worse outcome than holding the loop open for a wait the application asked
    // for. Every exit path below releases it.
    const release = (): void => {
      clearTimeout(timer);
      detachAbort();
    };

    if (signal !== undefined) {
      const onAbort = (): void => {
        release();
        reject(abortReason(signal));
      };
      if (signal.aborted) {
        onAbort();
        return;
      }
      signal.addEventListener('abort', onAbort, { once: true });
      detachAbort = (): void => signal.removeEventListener('abort', onAbort);
    }

    work.then(
      value => {
        release();
        resolve(value);
      },
      error => {
        release();
        reject(error);
      },
    );
  });
}

/**
 * What an aborted wait rejects with: the caller's own reason when they supplied
 * one, {@link KafkaReplyAbortedError} otherwise.
 *
 * `controller.abort()` with no argument does not leave `reason` empty — the
 * runtime fills it with a generic `AbortError` `DOMException`, which says
 * strictly less than this package's own error. Only a reason the caller chose is
 * worth forwarding.
 */
export function abortReason(signal: AbortSignal): unknown {
  const reason: unknown = signal.reason;
  if (reason instanceof Error && reason.name === 'AbortError') {
    return new KafkaReplyAbortedError();
  }
  return reason ?? new KafkaReplyAbortedError();
}
