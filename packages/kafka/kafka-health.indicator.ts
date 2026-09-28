import { Inject, Injectable, OnApplicationShutdown } from '@nestjs/common';
import { KafkaClientDriver, KafkaDriverAdmin } from './driver';
import { KAFKA_CLIENT_DRIVER } from './tokens';

/** Options for {@link KafkaHealthIndicator.isHealthy}. */
export interface KafkaHealthCheckOptions {
  /**
   * How long the metadata round trip may take before the cluster counts as
   * unreachable.
   *
   * @default 5000
   */
  timeoutMs?: number;
}

/** One indicator's status, with the details that explain it. */
export interface KafkaHealthStatus {
  status: 'up' | 'down';
  [detail: string]: unknown;
}

/**
 * A check's result, keyed by the name the caller gave it — the
 * `HealthIndicatorResult` shape `@nestjs/terminus` expects, so the indicator
 * plugs into `HealthCheckService.check()` without the package depending on
 * terminus.
 */
export type KafkaHealthIndicatorResult = Record<string, KafkaHealthStatus>;

/** The default {@link KafkaHealthCheckOptions.timeoutMs}. */
export const DEFAULT_KAFKA_HEALTH_TIMEOUT_MS = 5_000;

type ProbeOutcome =
  | { reachable: true; latencyMs: number; topics: number }
  | { reachable: false; message: string };

/**
 * Whether the Kafka cluster is reachable, for readiness probes and
 * `@nestjs/terminus`:
 *
 * ```ts
 * @Get('health')
 * @HealthCheck()
 * check() {
 *   return this.health.check([() => this.kafka.isHealthy('kafka')]);
 * }
 * ```
 *
 * The check is a real metadata round trip (`listTopics`) under a timeout. It is
 * never `connect()`: `librdkafka` connects lazily, and `connect()` resolves in
 * milliseconds against a broker that is down, so a connect-based probe would
 * report a dead cluster healthy.
 *
 * One admin client is reused while checks succeed. After a failure it is
 * discarded and the next check opens a fresh one — a client that failed while
 * the cluster was down keeps reporting that failure after it recovers.
 * Overlapping checks share one round trip, so a probe that fires faster than
 * the cluster answers never piles up clients.
 *
 * @publicApi
 */
@Injectable()
export class KafkaHealthIndicator implements OnApplicationShutdown {
  private admin?: Promise<KafkaDriverAdmin>;
  private inFlight?: Promise<ProbeOutcome>;

  constructor(
    @Inject(KAFKA_CLIENT_DRIVER) private readonly driver: KafkaClientDriver,
  ) {}

  /**
   * Check the cluster and report it under `key`: `up` with the round trip's
   * latency and the number of topics it returned, or `down` with the reason.
   * It resolves either way — a `down` result is what fails a terminus check.
   */
  async isHealthy(
    key = 'kafka',
    options: KafkaHealthCheckOptions = {},
  ): Promise<KafkaHealthIndicatorResult> {
    this.inFlight ??= this.probe(
      options.timeoutMs ?? DEFAULT_KAFKA_HEALTH_TIMEOUT_MS,
    ).finally(() => {
      this.inFlight = undefined;
    });
    const outcome = await this.inFlight;
    return {
      [key]: outcome.reachable
        ? { status: 'up', latencyMs: outcome.latencyMs, topics: outcome.topics }
        : { status: 'down', message: outcome.message },
    };
  }

  async onApplicationShutdown(): Promise<void> {
    const admin = this.admin;
    this.admin = undefined;
    await disconnect(admin);
  }

  private async probe(timeoutMs: number): Promise<ProbeOutcome> {
    const { createAdmin } = this.driver;
    if (!createAdmin) {
      return {
        reachable: false,
        message:
          'The Kafka driver cannot open an admin client, so the cluster ' +
          'cannot be reached to check it.',
      };
    }
    const started = Date.now();
    try {
      this.admin ??= connectAdmin(createAdmin.call(this.driver));
      const topics = await withTimeout(
        this.admin.then(admin => admin.listTopics()),
        timeoutMs,
      );
      return { reachable: true, latencyMs: Date.now() - started, topics: topics.length };
    } catch (error) {
      this.discardAdmin();
      return { reachable: false, message: describe(error) };
    }
  }

  /** Drop the admin client so the next check opens a fresh one. */
  private discardAdmin(): void {
    const admin = this.admin;
    this.admin = undefined;
    // In the background: a client wedged on a dead cluster must not hold up
    // the check that reports it.
    void disconnect(admin);
  }
}

async function connectAdmin(admin: KafkaDriverAdmin): Promise<KafkaDriverAdmin> {
  await admin.connect();
  return admin;
}

async function disconnect(admin: Promise<KafkaDriverAdmin> | undefined): Promise<void> {
  try {
    await (await admin)?.disconnect();
  } catch {
    // It never connected, or the cluster is gone; either way it is released.
  }
}

async function withTimeout<T>(work: Promise<T>, timeoutMs: number): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const timeout = new Promise<never>((_, reject) => {
    timer = setTimeout(
      () =>
        reject(
          new Error(`no cluster metadata within ${timeoutMs} ms`),
        ),
      timeoutMs,
    );
  });
  try {
    return await Promise.race([work, timeout]);
  } finally {
    clearTimeout(timer);
  }
}

function describe(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
