/**
 * A counting semaphore that caps how many messages a consumer processes at once.
 *
 * Kafka itself has no client-side flow control beyond fetch sizing, so a fast
 * broker can hand a consumer more in-flight work than slow handlers can keep up
 * with. {@link KafkaModuleOptions.maxInFlight} (and its per-consumer / per-handler
 * overrides) wires this semaphore in front of every dispatch so the number of
 * concurrently-running handlers never exceeds the configured permit count — the
 * backpressure the constitution and BRIEF §9 require.
 *
 * A permit count of `0` or below means "uncapped": {@link createBackpressure}
 * returns a no-op limiter so the common case adds no overhead.
 *
 * @internal
 */
export interface KafkaBackpressure {
  /**
   * Run `task` once a permit is available, releasing the permit when it settles
   * (whether it resolves or rejects). Calls beyond the permit count wait for an
   * earlier task to release.
   */
  run<T>(task: () => Promise<T>): Promise<T>;
}

class Semaphore implements KafkaBackpressure {
  private available: number;
  private readonly waiters: (() => void)[] = [];

  constructor(permits: number) {
    this.available = permits;
  }

  async run<T>(task: () => Promise<T>): Promise<T> {
    await this.acquire();
    try {
      return await task();
    } finally {
      this.release();
    }
  }

  private acquire(): Promise<void> {
    if (this.available > 0) {
      this.available -= 1;
      return Promise.resolve();
    }
    return new Promise<void>(resolve => {
      this.waiters.push(resolve);
    });
  }

  private release(): void {
    const next = this.waiters.shift();
    if (next) {
      // Hand the permit straight to the next waiter instead of bumping the count
      // and immediately decrementing it again.
      next();
      return;
    }
    this.available += 1;
  }
}

const UNCAPPED: KafkaBackpressure = {
  run: task => task(),
};

/**
 * Build a {@link KafkaBackpressure} limiter for `maxInFlight` permits. A value of
 * `0` or below disables the cap and returns a shared no-op limiter.
 *
 * @internal
 */
export function createBackpressure(maxInFlight: number): KafkaBackpressure {
  return maxInFlight > 0 ? new Semaphore(maxInFlight) : UNCAPPED;
}
