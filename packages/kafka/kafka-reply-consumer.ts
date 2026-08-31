import { randomUUID } from 'node:crypto';
import {
  KafkaClientDriver,
  KafkaDriverConsumer,
  KafkaEachMessagePayload,
  KafkaMessageHeaders,
} from './driver';
import { KafkaReply } from './interfaces';
import { deserializeKafkaValue } from './kafka-message-codec';
import { KafkaReplyAbortedError } from './kafka-request-reply.errors';
import {
  KAFKA_READINESS_PROBE_HEADER,
  KafkaReplySender,
  ResolvedRequestReplyOptions,
  readHeaderText,
} from './kafka-request-reply.protocol';
import { settledWithin, waitForReply } from './kafka-request-reply.waiting';

/**
 * How many sentinels the readiness probe spends its budget on. The resend
 * interval is the budget divided by this, so the default 10s budget re-produces
 * every 500ms — often enough that the first request after boot is not held up
 * once the group is assigned, rarely enough that an unassigned consumer is not
 * flooded — and a caller who shortens the budget shortens the cadence with it
 * instead of getting one attempt.
 */
const PROBE_ATTEMPTS = 20;

/**
 * A caller waiting on one correlation id.
 */
interface KafkaReplyWaiter {
  resolve: (reply: KafkaReply<unknown>) => void;
  reject: (error: unknown) => void;
}

/**
 * This instance's reply consumer: one extra Kafka consumer, in a consumer group
 * of its own, that fetches the whole shared reply topic and hands each reply to
 * whichever local caller is waiting on its correlation id.
 *
 * The routing decision (ADR 0001 §2) is the reason this class is so small.
 * Every instance runs a **single-member ephemeral group**
 * (`<groupIdPrefix><uuid>`), so there is nothing to rebalance: group membership
 * changes on other instances cannot move a reply away from the process that
 * asked for it. Replies fan out to every instance and the (N−1) that are not
 * waiting drop theirs on a map miss — the hot path, and deliberately silent,
 * because logging it would be self-DoS. The group commits no offsets and starts
 * at latest: a reply is a volatile signal to an in-memory promise, not durable
 * work, and the correlation map — not the log position — is the source of truth.
 *
 * ### Readiness
 *
 * `request()` must never produce before this consumer is assigned and fetching,
 * or a fast reply can land before the "latest" position is established and be
 * skipped forever. The mechanism here is a **sentinel self-message**: the
 * instance produces a valueless message to its own reply topic, carrying a
 * correlation id registered in the same pending map as a real request, and is
 * ready when it consumes that message back. It re-produces the sentinel until
 * one copy returns, because a sentinel is subject to the very race it is
 * checking for — see {@link KafkaReplyConsumer.runProbe}.
 *
 * That mechanism was chosen over polling `assignment()` or a rebalance callback
 * for three reasons. It proves the *end-to-end* property that actually matters
 * (a message produced now will be delivered to us), rather than a proxy for it.
 * It needs nothing from the driver surface beyond produce/consume, so no
 * client-specific hook enters {@link KafkaClientDriver} and the guarantee is
 * exercised by the in-memory broker in unit tests. And it doubles as a
 * permissions probe: a missing or unauthorized reply topic fails the produce and
 * surfaces as an error naming the topic, instead of the worst diagnostic there
 * is — a silent hang.
 *
 * @internal
 */
export class KafkaReplyConsumer {
  private readonly consumer: KafkaDriverConsumer;
  private readonly waiting = new Map<string, KafkaReplyWaiter>();

  /** The in-flight (or settled) readiness probe; re-armed after a failure. */
  private probe?: Promise<void>;

  /**
   * Bumped every time a probe is armed or expired. The running sentinel loop
   * compares against it, which is how a probe nobody is waiting for any more
   * stops producing.
   */
  private probeGeneration = 0;

  /**
   * This instance's ephemeral consumer group. Unique per process by
   * construction, which is the whole routing strategy in one string.
   */
  readonly groupId: string;

  constructor(
    driver: KafkaClientDriver,
    private readonly producer: KafkaReplySender,
    private readonly options: ResolvedRequestReplyOptions,
  ) {
    this.groupId = `${options.groupIdPrefix}${randomUUID()}`;
    this.consumer = driver.createConsumer({
      // Offsets for a group that dies with the process are pure
      // `__consumer_offsets` churn. The caller's config may override this; the
      // group id may not.
      'enable.auto.commit': false,
      // Start at latest, and say so **here** rather than on `subscribe()`: the
      // Confluent client accepts `fromBeginning` only at consumer creation and
      // rejects it as a subscribe option outright (`ERR__INVALID_ARG`). Replies
      // older than this process started are answers to requests nobody in it is
      // waiting for, so replaying them would be pure noise anyway.
      fromBeginning: false,
      ...options.consumer,
      groupId: this.groupId,
    });
  }

  /**
   * Connect, subscribe, and start fetching, then arm readiness without awaiting
   * it.
   */
  async start(): Promise<void> {
    await this.consumer.connect();
    await this.consumer.subscribe({ topics: [this.options.replyTopic] });
    await this.consumer.run({
      eachMessage: payload => this.onMessage(payload),
    });
    // Arm readiness eagerly so a brand-new group's initial rebalance delay
    // overlaps the rest of bootstrap instead of the first `request()`. Going
    // through `whenReady` rather than the probe directly means this unattended
    // warm-up is bounded by the same budget every other readiness wait is, so a
    // consumer that never becomes assigned stops producing sentinels instead of
    // trying forever.
    void this.whenReady().catch(() => {
      // Nobody is waiting on the eager warm-up: the first `request()` arms a
      // fresh probe and reports the failure to a caller who can act on it.
    });
  }

  /**
   * Settle every outstanding wait and disconnect.
   *
   * Draining "until the reply arrives" could hold shutdown for a full timeout
   * for work whose outcome is unknowable anyway, so pending requests fail fast —
   * the same stop-claims → drain → disconnect order the rest of the package
   * follows.
   */
  async stop(): Promise<void> {
    const waiters = [...this.waiting.values()];
    this.waiting.clear();
    for (const waiter of waiters) {
      waiter.reject(
        new KafkaReplyAbortedError(
          'The application is shutting down; the wait for a Kafka reply was ' +
            'abandoned. The request may still be processed.',
        ),
      );
    }
    await this.consumer.disconnect();
  }

  /**
   * Resolve once this instance's reply consumer is proven to be fetching,
   * bounded by `readinessTimeoutMs` and cancellable by the caller's signal.
   */
  whenReady(signal?: AbortSignal): Promise<void> {
    return waitForReply(
      this.ensureProbe(),
      this.options.readinessTimeoutMs,
      () => this.expireProbe(),
      signal,
    );
  }

  /**
   * Register interest in one correlation id. Callers register *before* producing
   * the request so a same-tick reply cannot race the map.
   */
  expect(correlationId: string): Promise<KafkaReply<unknown>> {
    return new Promise<KafkaReply<unknown>>((resolve, reject) => {
      this.waiting.set(correlationId, { resolve, reject });
    });
  }

  /**
   * Stop waiting on a correlation id. After this a reply for it is dropped like
   * any other unclaimed reply, which is exactly what makes a late reply (one
   * that arrives after a timeout or an abort) harmless.
   */
  forget(correlationId: string): void {
    this.waiting.delete(correlationId);
  }

  private async onMessage(payload: KafkaEachMessagePayload): Promise<void> {
    const headers = payload.message.headers;
    if (headers === undefined) {
      // A message with no headers at all carries no correlation id, so it is
      // not a reply — the reply topic is shared, and unrelated traffic on it is
      // the caller's business, not ours.
      return;
    }

    const correlationId = readHeaderText(
      headers,
      this.options.headers.correlationId,
    );
    if (correlationId === undefined) {
      return;
    }

    const waiter = this.waiting.get(correlationId);
    if (waiter === undefined) {
      // Another instance's reply, a duplicate, or a late one. This is the hot
      // path of the chosen routing — (N−1)/N of everything consumed here — so
      // it is silent by design.
      return;
    }
    this.waiting.delete(correlationId);
    waiter.resolve(this.toReply(payload, headers, correlationId));
  }

  private toReply(
    payload: KafkaEachMessagePayload,
    headers: KafkaMessageHeaders,
    correlationId: string,
  ): KafkaReply<unknown> {
    return {
      value: deserializeKafkaValue(payload.message.value),
      headers,
      correlationId,
      topic: payload.topic,
      partition: payload.partition,
      offset: payload.message.offset,
    };
  }

  private ensureProbe(): Promise<void> {
    if (this.probe === undefined) {
      this.probeGeneration += 1;
      const generation = this.probeGeneration;
      this.probe = this.runProbe(generation);
      // A probe that failed on a broker blip or a not-yet-created topic must not
      // poison the service for the process's lifetime: forget it so the next
      // request tries again. Guarded by generation, so a rejection arriving
      // after the probe was already expired cannot discard its replacement.
      this.probe.catch(() => this.retireProbe(generation));
    }
    return this.probe;
  }

  private retireProbe(generation: number): void {
    if (this.probeGeneration === generation) {
      this.probe = undefined;
    }
  }

  /**
   * Give up on readiness for now: retire the probe *and* end its sentinel loop
   * by moving the generation on, so the next request starts a fresh one. A
   * consumer that was not fetching a moment ago may be fetching now.
   */
  private expireProbe(): Error {
    this.probeGeneration += 1;
    this.probe = undefined;
    return this.readinessTimeoutError();
  }

  /**
   * Prove this consumer is fetching by round-tripping a sentinel through the
   * reply topic — re-producing it until one copy comes back.
   *
   * **One sentinel is not enough**, and that is the whole reason this loop
   * exists. The consumer starts at *latest*, so a sentinel produced before the
   * group's first assignment completes is written past the position the consumer
   * will take up and is never delivered — the exact race the probe exists to
   * close, turned on the probe itself. Re-producing is what makes it converge.
   * A single sentinel appears to work against an in-memory broker, where a
   * subscription is live the instant it is registered; against a real broker it
   * never returns.
   *
   * The loop ends when a sentinel returns, when the produce fails (a missing or
   * unwritable reply topic, reported with the topic named), when shutdown
   * rejects the pending sentinel, or when {@link expireProbe} moves the
   * generation on because a caller's readiness budget ran out.
   */
  private async runProbe(generation: number): Promise<void> {
    const correlationId = randomUUID();
    const delivered = this.expect(correlationId);
    try {
      while (this.probeGeneration === generation) {
        await this.sendSentinel(correlationId);
        if (await settledWithin(delivered, this.resendIntervalMs())) {
          return;
        }
      }
      throw this.readinessTimeoutError();
    } finally {
      this.forget(correlationId);
    }
  }

  private resendIntervalMs(): number {
    return this.options.readinessTimeoutMs / PROBE_ATTEMPTS;
  }

  private async sendSentinel(correlationId: string): Promise<void> {
    try {
      await this.producer.send({
        topic: this.options.replyTopic,
        messages: [
          {
            key: correlationId,
            value: null,
            headers: {
              [this.options.headers.correlationId]: correlationId,
              [KAFKA_READINESS_PROBE_HEADER]: this.groupId,
            },
          },
        ],
      });
    } catch (cause) {
      throw this.readinessProduceError(cause);
    }
  }

  private readinessTimeoutError(): Error {
    return new Error(
      `The Kafka reply consumer for "${this.options.replyTopic}" was not ` +
        `ready within ${this.options.readinessTimeoutMs}ms (consumer group ` +
        `"${this.groupId}"). Requests are not produced before the reply ` +
        'consumer is fetching, because a reply that arrives first would be ' +
        'lost. Check that the reply topic exists and that this application ' +
        'may read it.',
    );
  }

  private readinessProduceError(cause: unknown): Error {
    const error = new Error(
      `The Kafka reply topic "${this.options.replyTopic}" could not be ` +
        'written to while checking reply-consumer readiness. Check that the ' +
        'topic exists and that this application may write to it.',
    );
    (error as { cause?: unknown }).cause = cause;
    return error;
  }
}
