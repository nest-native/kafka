import { randomUUID } from 'node:crypto';
import {
  Inject,
  Injectable,
  OnApplicationBootstrap,
  OnApplicationShutdown,
} from '@nestjs/common';
import { KafkaClientDriver, KafkaProducerMessage } from './driver';
import {
  KafkaModuleOptions,
  KafkaReply,
  KafkaRequestOptions,
  KafkaRequestRecord,
} from './interfaces';
import { KafkaProducerService } from './kafka-producer.service';
import { KafkaReplyConsumer } from './kafka-reply-consumer';
import {
  KafkaReplyRemoteError,
  KafkaReplyTimeoutError,
} from './kafka-request-reply.errors';
import {
  ResolvedRequestReplyOptions,
  assertNoReservedHeaders,
  readHeaderText,
  resolveRequestReplyOptions,
} from './kafka-request-reply.protocol';
import { waitForReply } from './kafka-request-reply.waiting';
import { deserializeKafkaValue } from './kafka-message-codec';
import { KAFKA_CLIENT_DRIVER, KAFKA_MODULE_OPTIONS } from './tokens';

/**
 * What the service owns once `requestReply` is configured. Absent entirely when
 * it is not, so "configured" is a single check rather than a family of
 * optional fields that can disagree.
 */
interface KafkaRequestReplyRuntime {
  config: ResolvedRequestReplyOptions;
  replies: KafkaReplyConsumer;
}

/**
 * The client half of request-reply: produce a request and await the correlated
 * reply.
 *
 * This is an **opt-in migration bridge**, not the model the package recommends.
 * `@KafkaHandler` stays fire-and-forget and Kafka stays the event log it is;
 * what this adds is the piece a `@MessagePattern`/`ClientKafka.send()`
 * application cannot safely hand-roll — making a reply reach the *instance* that
 * asked, across rebalances, restarts, and N replicas behind a load balancer.
 *
 * Two properties are worth internalising before using it:
 *
 * - **The reply path is at-most-once.** A reply is only useful to one in-memory
 *   promise in one process; if that process died, the reply is noise. Requests
 *   stay at-least-once, replies do not.
 * - **A timeout means the outcome is unknown.** Kafka offers no "connection
 *   refused" — an unanswered request may still be processed later. Retrying is
 *   the caller's decision, because a transport-level retry on an unknown
 *   outcome manufactures duplicates.
 *
 * Injecting the service without configuring `requestReply` is safe and costs
 * nothing: no consumer is created, no topic is touched, and `request()` rejects
 * with a configuration error naming the missing option.
 *
 * @example
 * ```ts
 * const reply = await this.requests.request<TotalResult>({
 *   topic: 'orders.total',
 *   message: { value: JSON.stringify({ customerId }) },
 * });
 * return reply.value;
 * ```
 *
 * @publicApi
 */
@Injectable()
export class KafkaRequestReplyService
  implements OnApplicationBootstrap, OnApplicationShutdown
{
  private readonly runtime?: KafkaRequestReplyRuntime;
  private shuttingDown = false;

  constructor(
    @Inject(KAFKA_MODULE_OPTIONS) options: KafkaModuleOptions,
    @Inject(KAFKA_CLIENT_DRIVER) driver: KafkaClientDriver,
    private readonly producer: KafkaProducerService,
  ) {
    if (options.requestReply === undefined) {
      return;
    }
    const config = resolveRequestReplyOptions(options.requestReply);
    this.runtime = {
      config,
      replies: new KafkaReplyConsumer(driver, this.producer, config),
    };
  }

  /**
   * Start this instance's reply consumer. Runs after every `onModuleInit`, so
   * the shared producer is already connected and the readiness probe can go out
   * immediately.
   */
  async onApplicationBootstrap(): Promise<void> {
    await this.runtime?.replies.start();
  }

  /**
   * Fail every pending request fast and disconnect the reply consumer. Waiting
   * out the timeouts would hold shutdown for work whose outcome is unknowable
   * anyway.
   */
  async onApplicationShutdown(): Promise<void> {
    this.shuttingDown = true;
    await this.runtime?.replies.stop();
  }

  /**
   * Produce `record.message` to `record.topic` with a correlation id and this
   * instance's reply address stamped into its headers, then resolve with the
   * correlated reply.
   *
   * The method is named `request` rather than `sendAndReceive` because `send` in
   * this package already means "publish and return broker acks", and a second
   * `send*` verb with wholly different semantics would invite exactly the
   * confusion the producer/consumer split avoids.
   *
   * @throws KafkaReplyTimeoutError when no reply arrives in time — outcome
   * unknown, not "it did not happen".
   * @throws KafkaReplyRemoteError when the remote handler failed and its error
   * mapped to `'commit'`.
   * @throws KafkaReplyAbortedError (or `signal.reason`) when the wait is
   * cancelled by the caller or by shutdown.
   * @throws Error when `requestReply` is unconfigured, when a request header
   * collides with a reserved key, or when the reply consumer is not ready inside
   * `readinessTimeoutMs`.
   *
   * Producer failures while sending the request propagate untouched, exactly as
   * `send()`'s do.
   */
  async request<T = unknown>(
    record: KafkaRequestRecord,
    options: KafkaRequestOptions = {},
  ): Promise<KafkaReply<T>> {
    const { config, replies } = this.requireRuntime();
    assertNoReservedHeaders(record.message.headers, config.headers);

    // Contractual: never produce before this instance's reply consumer is
    // fetching, or a fast reply can land ahead of the consumer's position and be
    // skipped forever.
    await replies.whenReady(options.signal);

    return this.exchange<T>(record, options, config, replies);
  }

  private async exchange<T>(
    record: KafkaRequestRecord,
    options: KafkaRequestOptions,
    config: ResolvedRequestReplyOptions,
    replies: KafkaReplyConsumer,
  ): Promise<KafkaReply<T>> {
    const correlationId = randomUUID();
    const timeoutMs = options.timeoutMs ?? config.timeoutMs;
    // Registered before the produce so a same-tick reply cannot race the map.
    const delivered = replies.expect(correlationId);

    try {
      await this.producer.send({
        topic: record.topic,
        messages: [this.stampRequest(record.message, correlationId, config)],
      });
      const reply = await waitForReply(
        delivered,
        timeoutMs,
        () => new KafkaReplyTimeoutError(record.topic, correlationId, timeoutMs),
        options.signal,
      );
      return this.settle<T>(reply, config);
    } finally {
      // Whatever happened, stop claiming this correlation id: a reply that
      // arrives afterwards is dropped by the same map miss that drops another
      // instance's replies.
      replies.forget(correlationId);
    }
  }

  /**
   * Stamp the address onto the request.
   *
   * `replyPartition` is deliberately never written. This package's reply
   * consumer reads every partition of its reply topic, so targeting one buys
   * nothing — and `ServerKafka` treats a missing value as "no partition
   * targeting" and falls back to the default partitioner, which is exactly what
   * an un-migrated `@MessagePattern` service should do for us.
   */
  private stampRequest(
    message: KafkaProducerMessage,
    correlationId: string,
    config: ResolvedRequestReplyOptions,
  ): KafkaProducerMessage {
    return {
      ...message,
      headers: {
        ...message.headers,
        [config.headers.correlationId]: correlationId,
        [config.headers.replyTopic]: config.replyTopic,
      },
    };
  }

  /**
   * Turn a delivered reply into the caller's result. Acceptance of an error
   * reply is presence-based — any non-absent error header is an error — which
   * tolerates this package's `{ name, message }` payload and the official
   * transport's serialized error alike.
   */
  private settle<T>(
    reply: KafkaReply<unknown>,
    config: ResolvedRequestReplyOptions,
  ): KafkaReply<T> {
    const detail = readHeaderText(reply.headers, config.headers.error);
    if (detail === undefined) {
      return reply as KafkaReply<T>;
    }
    throw new KafkaReplyRemoteError(
      deserializeKafkaValue(detail),
      reply,
      detail,
    );
  }

  private requireRuntime(): KafkaRequestReplyRuntime {
    if (this.runtime === undefined) {
      throw new Error(
        'Kafka request-reply is not configured. Pass a "requestReply" block ' +
          'with a "replyTopic" to KafkaModule.forRoot/forRootAsync before ' +
          'calling request().',
      );
    }
    if (this.shuttingDown) {
      throw new Error(
        'The application is shutting down; no new Kafka requests are accepted.',
      );
    }
    return this.runtime;
  }
}
