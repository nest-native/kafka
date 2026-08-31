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
import { waitForReply } from './kafka-request-reply.waiting';

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
 * instance produces one valueless message to its own reply topic, carrying a
 * correlation id registered in the same pending map as a real request, and is
 * ready when it consumes that message back.
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
      ...options.consumer,
      groupId: this.groupId,
    });
  }

  /**
   * Connect, subscribe from latest, and start fetching. The readiness probe is
   * kicked off without being awaited so a brand-new group's initial rebalance
   * delay overlaps the rest of bootstrap instead of the first `request()`.
   */
  async start(): Promise<void> {
    await this.consumer.connect();
    await this.consumer.subscribe({
      topics: [this.options.replyTopic],
      fromBeginning: false,
    });
    await this.consumer.run({
      eachMessage: payload => this.onMessage(payload),
    });
    void this.ensureProbe();
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
      () => this.readinessTimeoutError(),
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
      this.probe = this.runProbe();
      // A probe that failed on a broker blip or a not-yet-created topic must not
      // poison the service for the process's lifetime: forget it so the next
      // request tries again. (A probe still *pending* is reused — re-producing
      // sentinels would not make an unassigned consumer assigned any sooner.)
      this.probe.catch(() => {
        this.probe = undefined;
      });
    }
    return this.probe;
  }

  private async runProbe(): Promise<void> {
    const correlationId = randomUUID();
    const delivered = this.expect(correlationId);
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
      this.forget(correlationId);
      throw this.readinessProduceError(cause);
    }
    await delivered;
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
