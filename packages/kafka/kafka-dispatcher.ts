import {
  KafkaConsumerBatch,
  KafkaEachBatchPayload,
  KafkaEachMessagePayload,
} from './driver';
import {
  KafkaBackpressure,
  createBackpressure,
} from './kafka-backpressure';
import {
  KafkaBatchContext,
  KafkaContext,
  KafkaIncomingMessage,
} from './kafka-context';
import {
  applyKafkaErrorBehavior,
  KafkaErrorMapper,
} from './kafka-error-mapping';
import { KafkaHandlerInvocation } from './kafka-context-creator';
import { deserializeKafkaValue } from './kafka-message-codec';
import { KafkaReplyPublisher } from './kafka-reply-publisher';
import { KafkaReplyOutcome } from './kafka-request-reply.protocol';

/**
 * One discovered handler reduced to what the dispatcher needs: the runner that
 * drives the Nest enhancer pipeline for one message (or batch), and whether the
 * handler answers its requests.
 */
export interface DispatchHandler {
  run: (invocation: KafkaHandlerInvocation) => Promise<unknown>;
  /**
   * `true` for a `reply: true` handler. Guaranteed false for batch handlers —
   * the combination is rejected at bootstrap.
   */
  reply: boolean;
}

/**
 * Routes consumed messages and batches to their handlers through the Nest
 * enhancer pipeline, applying backpressure and tracking in-flight work so
 * graceful shutdown can drain it.
 *
 * One dispatcher backs one Kafka consumer. It owns the cross-cutting consumption
 * concerns the constitution and BRIEF §9 require — backpressure
 * ({@link KafkaBackpressure}), batch offset resolution (only after a batch is
 * handled), and error mapping — so the explorer stays focused on discovery and
 * wiring.
 *
 * @internal
 */
export class KafkaDispatcher {
  private readonly backpressure: KafkaBackpressure;

  /**
   * Messages/batches currently being handled. Graceful shutdown drains this set
   * before disconnecting so an in-flight handler is never interrupted.
   */
  private readonly inFlight = new Set<Promise<unknown>>();

  /**
   * Once shutdown begins the dispatcher stops accepting newly delivered records
   * (stop new claims → drain in-flight → disconnect).
   */
  private shuttingDown = false;

  constructor(
    private readonly routes: Map<string, DispatchHandler[]>,
    private readonly errorMapper: KafkaErrorMapper,
    maxInFlight: number,
    private readonly replier: KafkaReplyPublisher,
  ) {
    this.backpressure = createBackpressure(maxInFlight);
  }

  /**
   * Dispatch one consumed message to every handler routed to its topic.
   *
   * Once shutdown has begun the record is rejected, not ignored: returning
   * normally tells Confluent's client the record was processed, and it stores
   * the offset and commits it on the way out — until 0.5.1 a record fetched
   * during the drain was lost that way. Rejecting makes the client seek back,
   * so the next member of the group receives it. A record for a topic no
   * handler routes is acknowledged instead: nothing will ever handle it, and
   * leaving it uncommitted would stall the partition.
   */
  eachMessage(payload: KafkaEachMessagePayload): Promise<void> {
    if (this.shuttingDown) {
      return Promise.reject(shuttingDownError(payload.topic, payload.partition));
    }
    const matched = this.routes.get(payload.topic);
    if (!matched) {
      return Promise.resolve();
    }
    const invocation = this.toMessageInvocation(payload);
    return this.track(matched, invocation);
  }

  /**
   * Dispatch one fetched batch to every handler routed to its topic, then
   * resolve its offsets — only once every handler has returned, or failed with
   * an error mapped to `'commit'`. A `'retry'`-mapped failure rejects before
   * anything is resolved, so the client seeks back to the batch's first message
   * and the broker hands the whole batch back.
   *
   * The unit of work is the batch: a handler receives every message at once,
   * so the transport cannot know which of them it finished before failing, and
   * must never claim more than it did. Resolving while the batch was decoded,
   * as this did until 0.5.1, told the client the batch was processed before the
   * handler ran — a failed batch was committed and never redelivered.
   *
   * A batch delivered after shutdown began is left unresolved, which already
   * makes the client seek back to its first message without logging an error;
   * a batch for a topic no handler routes is acknowledged, as in
   * {@link eachMessage}.
   */
  eachBatch(payload: KafkaEachBatchPayload): Promise<void> {
    if (this.shuttingDown) {
      return Promise.resolve();
    }
    const matched = this.routes.get(payload.batch.topic);
    if (!matched) {
      resolveBatch(payload);
      return Promise.resolve();
    }
    const invocation = this.toBatchInvocation(payload.batch);
    return this.track(matched, invocation, () => resolveBatch(payload));
  }

  /**
   * Stop accepting records, then wait for every in-flight message/batch to
   * settle. A record delivered from here on is handed back to the client
   * rather than acknowledged (see {@link eachMessage}).
   */
  async drain(): Promise<void> {
    this.shuttingDown = true;
    await Promise.allSettled([...this.inFlight]);
  }

  /**
   * Run the matched handlers under backpressure and track the work for
   * graceful shutdown. `onHandled` runs inside the tracked work, after every
   * handler settled without a `'retry'` — so a drain waits for it too.
   */
  private track(
    matched: DispatchHandler[],
    invocation: KafkaHandlerInvocation,
    onHandled?: () => void,
  ): Promise<void> {
    const work = this.backpressure.run(async () => {
      await this.runHandlers(matched, invocation);
      onHandled?.();
    });
    this.inFlight.add(work);
    const forget = (): void => {
      this.inFlight.delete(work);
    };
    work.then(forget, forget);
    return work;
  }

  private async runHandlers(
    matched: DispatchHandler[],
    invocation: KafkaHandlerInvocation,
  ): Promise<void> {
    for (const handler of matched) {
      const outcome = await this.invokeHandler(handler, invocation);
      await this.publishReply(handler, invocation, outcome);
    }
  }

  /**
   * Run one handler and reduce it to a reply outcome.
   *
   * A `'retry'`-mapped failure throws out of here, which is what leaves the
   * offset uncommitted and stops the loop — so no reply is produced for work
   * the broker is about to redeliver. A `'commit'`-mapped failure is a final
   * answer, and becomes an error reply for handlers that reply.
   */
  private async invokeHandler(
    handler: DispatchHandler,
    invocation: KafkaHandlerInvocation,
  ): Promise<KafkaReplyOutcome> {
    try {
      return { status: 'value', value: await handler.run(invocation) };
    } catch (error) {
      // The handler's `@UseFilters` pipeline already ran; an error here means
      // no filter handled it. Map it to commit-or-retry instead of letting it
      // swallow silently (`nestjs/nest#9679`) or crash the consumer.
      applyKafkaErrorBehavior(error, invocation.context, this.errorMapper);
      return { status: 'error', error };
    }
  }

  private async publishReply(
    handler: DispatchHandler,
    invocation: KafkaHandlerInvocation,
    outcome: KafkaReplyOutcome,
  ): Promise<void> {
    if (!handler.reply) {
      return;
    }
    try {
      // `reply: true` and `batch: true` are mutually exclusive (rejected at
      // bootstrap), so a replying handler always runs on the per-message path
      // and its context is a `KafkaContext`.
      await this.replier.publish(invocation.context as KafkaContext, outcome);
    } catch (error) {
      // An unsent reply is unprocessed work from the requester's point of view,
      // so it goes through the same mapper as any other failure — redelivery by
      // default, another chance to answer inside the timeout window.
      applyKafkaErrorBehavior(error, invocation.context, this.errorMapper);
    }
  }

  private toMessageInvocation(
    payload: KafkaEachMessagePayload,
  ): KafkaHandlerInvocation {
    const message: KafkaIncomingMessage = payload.message;
    const context = new KafkaContext(
      payload.topic,
      payload.partition,
      message,
    );
    return { payload: deserializeKafkaValue(message.value), context };
  }

  private toBatchInvocation(batch: KafkaConsumerBatch): KafkaHandlerInvocation {
    const messages = batch.messages.map(message =>
      deserializeKafkaValue(message.value),
    );
    return { payload: messages, context: new KafkaBatchContext(batch) };
  }
}

function shuttingDownError(topic: string, partition: number): Error {
  return new Error(
    `Kafka consumer is shutting down; the record from ${topic}[${partition}] ` +
      'was not handled and is left for the next member of the group.',
  );
}

/**
 * Mark every message of a handled batch as processed, so the client commits
 * past it. Only called once the batch's handlers have settled without a
 * `'retry'`; a message the driver delivered without an offset has nothing to
 * resolve.
 */
function resolveBatch(payload: KafkaEachBatchPayload): void {
  for (const message of payload.batch.messages) {
    if (message.offset !== undefined) {
      payload.resolveOffset(message.offset);
    }
  }
}
