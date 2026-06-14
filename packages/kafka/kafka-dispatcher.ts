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

/**
 * One discovered handler reduced to what the dispatcher needs: the runner that
 * drives the Nest enhancer pipeline for one message (or batch).
 */
export interface DispatchHandler {
  run: (invocation: KafkaHandlerInvocation) => Promise<unknown>;
}

/**
 * Routes consumed messages and batches to their handlers through the Nest
 * enhancer pipeline, applying backpressure and tracking in-flight work so
 * graceful shutdown can drain it.
 *
 * One dispatcher backs one Kafka consumer. It owns the cross-cutting consumption
 * concerns the constitution and BRIEF §9 require — backpressure
 * ({@link KafkaBackpressure}), rebalance-safe offset resolution (batch mode), and
 * error mapping — so the explorer stays focused on discovery and wiring.
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
  ) {
    this.backpressure = createBackpressure(maxInFlight);
  }

  /**
   * Dispatch one consumed message to every handler routed to its topic.
   */
  eachMessage(payload: KafkaEachMessagePayload): Promise<void> {
    const matched = this.match(payload.topic);
    if (!matched) {
      return Promise.resolve();
    }
    const invocation = this.toMessageInvocation(payload);
    return this.track(matched, invocation);
  }

  /**
   * Dispatch one fetched batch to every handler routed to its topic, resolving
   * each message's offset as the batch is built so a rebalance mid-batch keeps
   * the progress already made (`nestjs/nest#12355`).
   */
  eachBatch(payload: KafkaEachBatchPayload): Promise<void> {
    const matched = this.match(payload.batch.topic);
    if (!matched) {
      return Promise.resolve();
    }
    const invocation = this.toBatchInvocation(payload);
    return this.track(matched, invocation);
  }

  /**
   * Wait for every in-flight message/batch to settle, then mark shutdown so no
   * further records are accepted.
   */
  async drain(): Promise<void> {
    this.shuttingDown = true;
    await Promise.allSettled([...this.inFlight]);
  }

  /**
   * The matching handlers for a topic, or `undefined` when shutting down or the
   * topic is unrouted (a record the broker delivered for a topic this consumer
   * does not own — ignored so its offset stays uncommitted).
   */
  private match(topic: string): DispatchHandler[] | undefined {
    if (this.shuttingDown) {
      return undefined;
    }
    return this.routes.get(topic);
  }

  private track(
    matched: DispatchHandler[],
    invocation: KafkaHandlerInvocation,
  ): Promise<void> {
    const work = this.backpressure.run(() =>
      this.runHandlers(matched, invocation),
    );
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
      try {
        await handler.run(invocation);
      } catch (error) {
        // The handler's `@UseFilters` pipeline already ran; an error here means
        // no filter handled it. Map it to commit-or-retry instead of letting it
        // swallow silently (`nestjs/nest#9679`) or crash the consumer.
        applyKafkaErrorBehavior(error, invocation.context, this.errorMapper);
      }
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

  private toBatchInvocation(
    payload: KafkaEachBatchPayload,
  ): KafkaHandlerInvocation {
    const batch: KafkaConsumerBatch = payload.batch;
    const messages: unknown[] = [];
    for (const message of batch.messages) {
      messages.push(deserializeKafkaValue(message.value));
      if (message.offset !== undefined) {
        // Resolve each offset as it is decoded so a partition revoked mid-batch
        // commits the progress already made rather than replaying the batch.
        payload.resolveOffset(message.offset);
      }
    }
    return { payload: messages, context: new KafkaBatchContext(batch) };
  }
}
