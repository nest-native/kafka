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
 * Resolve `true` when `work` settles inside `ms`, `false` when the interval
 * elapses first — a peek at a promise that keeps the promise alive either way.
 *
 * The readiness probe uses it to pace its sentinel loop: it needs to ask "did
 * the sentinel come back yet?" without abandoning the wait when the answer is
 * no, which neither `waitForReply` (it rejects) nor a bare `Promise.race` (it
 * leaks the loser's timer) can do.
 *
 * A rejection propagates, because the only thing that rejects a pending reply is
 * shutdown, and a loop must not keep producing sentinels through it.
 *
 * The interval timer is `unref`ed: it is internal pacing, not a wait the
 * application asked for, so it must never be the reason a process stays alive.
 *
 * @internal
 */
export function settledWithin(
  work: Promise<unknown>,
  ms: number,
): Promise<boolean> {
  return new Promise<boolean>((resolve, reject) => {
    const timer = setTimeout(() => resolve(false), ms);
    timer.unref();
    work.then(
      () => {
        clearTimeout(timer);
        resolve(true);
      },
      (error: unknown) => {
        clearTimeout(timer);
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
