import { Logger } from '@nestjs/common';
import { KafkaTopicPartitions } from './driver';

/**
 * How long a partition waits before a record whose failure maps to `'retry'` is
 * redelivered. The delay starts at `initialDelayMs` and grows by `multiplier`
 * with every consecutive failure of the same record, up to `maxDelayMs`.
 */
export interface KafkaRetryBackoffOptions {
  /**
   * Delay before the first redelivery.
   *
   * @default 1000
   */
  initialDelayMs?: number;

  /**
   * Upper bound for the delay. Once a failing record reaches it, every further
   * redelivery waits this long.
   *
   * @default 30000
   */
  maxDelayMs?: number;

  /**
   * Growth factor applied per consecutive failure of the same record. `1`
   * keeps the delay fixed.
   *
   * @default 2
   */
  multiplier?: number;
}

/** The defaults behind {@link KafkaRetryBackoffOptions}. */
export const DEFAULT_KAFKA_RETRY_BACKOFF: Readonly<
  Required<KafkaRetryBackoffOptions>
> = Object.freeze({
  initialDelayMs: 1_000,
  maxDelayMs: 30_000,
  multiplier: 2,
});

/**
 * Validate and complete the `retryBackoff` module option. `false` turns the
 * backoff off; `undefined` (the default) or an object turns it on, with
 * {@link DEFAULT_KAFKA_RETRY_BACKOFF} filling the gaps.
 *
 * @internal
 */
export function resolveRetryBackoff(
  options: KafkaRetryBackoffOptions | false | undefined,
): Required<KafkaRetryBackoffOptions> | undefined {
  if (options === false) {
    return undefined;
  }
  const resolved = { ...DEFAULT_KAFKA_RETRY_BACKOFF, ...options };
  const { initialDelayMs, maxDelayMs, multiplier } = resolved;
  if (!(initialDelayMs >= 0) || !(maxDelayMs >= initialDelayMs) || !(multiplier >= 1)) {
    throw new Error(
      'Invalid retryBackoff: expected initialDelayMs >= 0, maxDelayMs >= ' +
        `initialDelayMs and multiplier >= 1, got ${JSON.stringify(resolved)}.`,
    );
  }
  return resolved;
}

/**
 * The slice of a consumer the backoff needs: pausing and resuming partitions.
 *
 * @internal
 */
export interface KafkaPartitionPauser {
  pause(topics: KafkaTopicPartitions[]): void;
  resume(topics: KafkaTopicPartitions[]): void;
}

interface FailureStreak {
  /** The record (or the first record of the batch) that keeps failing. */
  offset: string | undefined;
  attempts: number;
  timer?: ReturnType<typeof setTimeout>;
}

/**
 * Spaces out the redelivery of records whose failure maps to `'retry'`.
 *
 * Confluent's client redelivers such a record by seeking back to it, and the
 * next fetch hands it over again — measured at a flat ~505 ms apart on a real
 * broker, forever, which hammers whatever dependency is failing. Sleeping in
 * the handler path instead would hold the consumer's worker and stall every
 * other partition it serves. So the backoff pauses only the failing partition,
 * lets the client seek back, and resumes the partition when the delay is up;
 * the other partitions keep flowing meanwhile.
 *
 * It never gives up on a record: turning a retry into a commit is the error
 * mapper's decision, not the transport's.
 *
 * @internal
 */
export class KafkaRetryBackoff {
  private readonly logger = new Logger(KafkaRetryBackoff.name);
  private readonly streaks = new Map<string, FailureStreak>();

  constructor(
    private readonly options: Required<KafkaRetryBackoffOptions>,
    private readonly pauser: KafkaPartitionPauser,
  ) {}

  /**
   * A record — or a batch starting at `offset` — failed with `'retry'`: pause
   * its partition for the streak's next delay.
   */
  failed(topic: string, partition: number, offset: string | undefined): void {
    const key = streakKey(topic, partition);
    const streak = this.nextStreak(this.streaks.get(key), offset);
    this.streaks.set(key, streak);

    const delayMs = this.delayFor(streak.attempts);
    const partitions = [{ topic, partitions: [partition] }];
    try {
      this.pauser.pause(partitions);
    } catch (error) {
      // Without a pause the client redelivers as it always did; the retry
      // still happens, only without the delay.
      this.logger.warn(
        `Could not pause ${topic}[${partition}] to back off a retry: ${describe(error)}`,
      );
      return;
    }
    this.logger.warn(
      `Retrying ${topic}[${partition}]${offset === undefined ? '' : `@${offset}`} ` +
        `in ${delayMs} ms (attempt ${streak.attempts + 1}).`,
    );
    streak.timer = setTimeout(() => this.resume(key, partitions), delayMs);
    streak.timer.unref();
  }

  /** The partition handled a record: its failure streak is over. */
  succeeded(topic: string, partition: number): void {
    const key = streakKey(topic, partition);
    const streak = this.streaks.get(key);
    if (streak) {
      clearTimeout(streak.timer);
      this.streaks.delete(key);
    }
  }

  /**
   * Forget every streak and cancel every pending resume — graceful shutdown is
   * about to disconnect the consumer, and a resume after that would throw.
   */
  cancelAll(): void {
    for (const streak of this.streaks.values()) {
      clearTimeout(streak.timer);
    }
    this.streaks.clear();
  }

  private nextStreak(
    previous: FailureStreak | undefined,
    offset: string | undefined,
  ): FailureStreak {
    if (previous && previous.offset === offset) {
      clearTimeout(previous.timer);
      return { offset, attempts: previous.attempts + 1 };
    }
    return { offset, attempts: 1 };
  }

  private delayFor(attempts: number): number {
    const { initialDelayMs, maxDelayMs, multiplier } = this.options;
    return Math.min(maxDelayMs, initialDelayMs * multiplier ** (attempts - 1));
  }

  private resume(key: string, partitions: KafkaTopicPartitions[]): void {
    const streak = this.streaks.get(key);
    if (streak) {
      streak.timer = undefined;
    }
    try {
      this.pauser.resume(partitions);
    } catch (error) {
      // The partition may have been revoked, or the consumer disconnected;
      // either way there is nothing left to resume here.
      this.logger.debug(
        `Could not resume ${partitions[0].topic} after a backoff: ${describe(error)}`,
      );
    }
  }
}

function streakKey(topic: string, partition: number): string {
  return `${topic}\u0000${partition}`;
}

function describe(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
