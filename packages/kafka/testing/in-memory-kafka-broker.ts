import {
  KafkaClientDriver,
  KafkaConsumerConfig,
  KafkaConsumerMessage,
  KafkaDriverConsumer,
  KafkaDriverProducer,
  KafkaEachBatchHandler,
  KafkaEachMessageHandler,
  KafkaProducerMessage,
  KafkaRecordMetadata,
  KafkaSendBatch,
  KafkaSendRecord,
  KafkaTransaction,
} from '../driver';

/**
 * A single message recorded by the broker as it was published, together with the
 * topic it was sent to. {@link InMemoryKafkaBroker.getSent} returns these so a
 * test can assert exactly what a handler (or a service) produced without a real
 * broker.
 */
export interface RecordedKafkaMessage {
  topic: string;
  message: KafkaProducerMessage;
}

/**
 * The internal bookkeeping for one consumer the broker created: the topics it
 * subscribed to and the callbacks it is running.
 */
interface ConsumerRegistration {
  topics: Set<string>;
  eachMessage?: KafkaEachMessageHandler;
  eachBatch?: KafkaEachBatchHandler;
}

/**
 * An in-memory Kafka broker that loops produced messages straight to the
 * consumers subscribed to their topic — the transport behind
 * {@link KafkaTestModule}.
 *
 * It lets a test drive the full `@KafkaConsumer` / `@KafkaHandler` pipeline
 * (guards, interceptors, pipes, filters) in both per-message and batch
 * (`eachBatch`) modes without a real Kafka broker or the native `librdkafka`
 * install — exactly the "in-memory transport for unit tests" the public API
 * promises. The same broker backs the producer service, so producing inside a
 * test delivers to the test's consumers, and every produced message is recorded
 * for assertions. {@link idle} awaits every in-flight handler pipeline —
 * including cascades a handler triggers — so tests settle deterministically
 * instead of sleeping.
 *
 * Plug it into a module through {@link InMemoryKafkaBroker.createDriverFactory}
 * (what {@link KafkaTestModule} does) or reach for it directly when you want to
 * inspect or inject messages around an application you wired yourself.
 *
 * @publicApi
 */
export class InMemoryKafkaBroker {
  private readonly consumers: ConsumerRegistration[] = [];
  private readonly sent: RecordedKafkaMessage[] = [];

  /**
   * Deliveries whose consumer pipelines have not settled yet. Every produce —
   * {@link emit}, a producer `send`/`sendBatch`, a committed transaction — is
   * tracked here from the moment it is initiated, so {@link idle} can await the
   * work even when the caller never does (a fire-and-forget send inside a
   * handler, for example).
   */
  private readonly inFlight = new Set<Promise<void>>();

  /**
   * A {@link KafkaDriverFactory} bound to this broker, suitable for
   * {@link KafkaModuleOptions.driverFactory}. {@link KafkaTestModule} passes it
   * automatically; supply it yourself when wiring {@link KafkaModule.forRoot}
   * with a broker you want to inspect.
   */
  createDriverFactory(): () => KafkaClientDriver {
    return () => this.createDriver();
  }

  /**
   * Build the {@link KafkaClientDriver} this broker backs: producers loop their
   * messages to subscribed consumers and consumers register their callbacks.
   */
  createDriver(): KafkaClientDriver {
    return {
      createProducer: () => this.createProducer(),
      createConsumer: (config?: KafkaConsumerConfig) =>
        this.createConsumer(config),
    };
  }

  /**
   * Inject a message onto a topic as if the broker had received it from an
   * external producer, dispatching it to every subscribed consumer. Use it to
   * exercise a consumer in isolation without a producing service.
   *
   * The message is recorded the same way a produced message is, so it appears in
   * {@link getSent}.
   *
   * The returned promise resolves when the subscribed consumers' pipelines have
   * settled for *this* delivery. Follow it with {@link idle} to also wait for
   * work those handlers merely started (fire-and-forget produces and the
   * consumers they trigger).
   */
  async emit(topic: string, message: KafkaProducerMessage): Promise<void> {
    await this.deliver(topic, [message]);
  }

  /**
   * Every message the broker has delivered, in order, paired with its topic.
   * Filter by topic with {@link getSentTo}.
   */
  getSent(): RecordedKafkaMessage[] {
    return [...this.sent];
  }

  /**
   * The messages delivered to a single topic, in order.
   */
  getSentTo(topic: string): KafkaProducerMessage[] {
    return this.sent
      .filter(record => record.topic === topic)
      .map(record => record.message);
  }

  /**
   * Resolve once every in-flight consumer pipeline has settled — the awaitable
   * settle point for tests that drive async `@KafkaHandler`s.
   *
   * Awaiting {@link emit} (or a producer send) already waits for the pipelines
   * that delivery runs, but not for work it only *starts*: a handler that
   * produces without awaiting the send (a fire-and-forget audit or DLQ produce,
   * say) leaves that follow-up dispatch running after `emit` resolves. `idle`
   * closes the gap — it keeps waiting until the broker is quiet, **including
   * dispatches triggered while it waits** (a handler producing to a topic
   * another consumer handles, and so on down the chain), so a test never needs
   * a `sleep` to let handlers finish:
   *
   * ```ts
   * await broker.emit('orders.placed', { value: JSON.stringify(order) });
   * await broker.idle(); // every handler pipeline (and cascade) has settled
   * assert.equal(broker.getSentTo('orders.audit').length, 1);
   * ```
   *
   * It resolves even when handlers throw (a rejected pipeline is settled, and
   * the transport's error mapping already decided commit-vs-retry), and it does
   * not stop consumption — unlike graceful shutdown's drain, the broker keeps
   * delivering afterwards. Work a handler schedules *outside* the dispatch
   * chain (a bare `setTimeout`, an untracked queue) is invisible to the broker
   * and is not awaited.
   */
  async idle(): Promise<void> {
    do {
      // Yield a macrotask first so a dispatch that is still crossing the
      // microtask queue (a fire-and-forget `producer.send(...)` whose
      // connect/serialize steps have not reached the broker yet) lands in
      // `inFlight` before the emptiness check.
      await new Promise<void>(resolve => setImmediate(resolve));
      await Promise.allSettled([...this.inFlight]);
    } while (this.inFlight.size > 0);
  }

  /**
   * Forget every recorded message. The registered consumers stay subscribed, so
   * this resets assertions between phases of one test without re-wiring.
   */
  reset(): void {
    this.sent.length = 0;
  }

  private createProducer(): KafkaDriverProducer {
    const sendRecord = (
      record: KafkaSendRecord,
    ): Promise<KafkaRecordMetadata[]> => this.send(record);
    const sendBatch = (batch: KafkaSendBatch): Promise<KafkaRecordMetadata[]> =>
      this.sendBatch(batch);

    return {
      connect: async () => {},
      disconnect: async () => {},
      send: sendRecord,
      sendBatch,
      transaction: async () => this.createTransaction(sendRecord, sendBatch),
    };
  }

  private async send(
    record: KafkaSendRecord,
  ): Promise<KafkaRecordMetadata[]> {
    await this.deliver(record.topic, record.messages);
    return record.messages.map((_, index) => metadata(record.topic, index));
  }

  private async sendBatch(
    batch: KafkaSendBatch,
  ): Promise<KafkaRecordMetadata[]> {
    const results: KafkaRecordMetadata[] = [];
    for (const topicMessages of batch.topicMessages ?? []) {
      await this.deliver(topicMessages.topic, topicMessages.messages);
      topicMessages.messages.forEach((_, index) =>
        results.push(metadata(topicMessages.topic, index)),
      );
    }
    return results;
  }

  private createConsumer(_config?: KafkaConsumerConfig): KafkaDriverConsumer {
    const registration: ConsumerRegistration = { topics: new Set() };
    this.consumers.push(registration);

    return {
      connect: async () => {},
      disconnect: async () => {
        registration.topics.clear();
        registration.eachMessage = undefined;
        registration.eachBatch = undefined;
      },
      subscribe: async subscription => {
        for (const topic of subscription.topics) {
          registration.topics.add(topic);
        }
      },
      run: async config => {
        registration.eachMessage = config.eachMessage;
        registration.eachBatch = config.eachBatch;
      },
    };
  }

  /**
   * A transaction that buffers writes and flushes them on commit, discarding
   * them on abort — the all-or-nothing delivery a real transaction guarantees,
   * so a test can prove a transactional handler commits on success and delivers
   * nothing on failure.
   */
  private createTransaction(
    sendRecord: (record: KafkaSendRecord) => Promise<KafkaRecordMetadata[]>,
    sendBatch: (batch: KafkaSendBatch) => Promise<KafkaRecordMetadata[]>,
  ): KafkaTransaction {
    const pending: Array<() => Promise<unknown>> = [];

    return {
      send: async record => {
        pending.push(() => sendRecord(record));
        return record.messages.map((_, index) => metadata(record.topic, index));
      },
      sendBatch: async batch => {
        pending.push(() => sendBatch(batch));
        return [];
      },
      // Offsets are an external concern for the in-memory broker: it does not
      // model a commit log, so committing offsets is a no-op here.
      sendOffsets: async () => {},
      commit: async () => {
        for (const flush of pending) {
          await flush();
        }
      },
      abort: async () => {
        pending.length = 0;
      },
    };
  }

  /**
   * Run one delivery and track it in {@link InMemoryKafkaBroker.inFlight} until
   * it settles, mirroring how the consumer runtime's dispatcher tracks its own
   * in-flight work for graceful shutdown. The consumer callbacks awaited by
   * {@link dispatch} resolve when the full `@KafkaHandler` pipeline settles, so
   * a tracked delivery *is* the handler work — {@link idle} needs nothing from
   * the production path. Tracking starts synchronously with the produce call,
   * before any `await`, so work initiated inside a still-running handler is
   * never missed.
   */
  private deliver(
    topic: string,
    messages: KafkaProducerMessage[],
  ): Promise<void> {
    const work = this.dispatch(topic, messages);
    this.inFlight.add(work);
    const forget = (): void => {
      this.inFlight.delete(work);
    };
    work.then(forget, forget);
    return work;
  }

  private async dispatch(
    topic: string,
    messages: KafkaProducerMessage[],
  ): Promise<void> {
    for (const message of messages) {
      this.sent.push({ topic, message });
    }
    for (const consumer of this.consumers) {
      if (consumer.topics.has(topic)) {
        await this.deliverToConsumer(consumer, topic, messages);
      }
    }
  }

  private async deliverToConsumer(
    consumer: ConsumerRegistration,
    topic: string,
    messages: KafkaProducerMessage[],
  ): Promise<void> {
    if (consumer.eachBatch) {
      await this.deliverBatch(consumer, topic, messages);
      return;
    }
    await this.deliverEach(consumer, topic, messages);
  }

  private async deliverEach(
    consumer: ConsumerRegistration,
    topic: string,
    messages: KafkaProducerMessage[],
  ): Promise<void> {
    for (let partition = 0; partition < messages.length; partition += 1) {
      // Producers and consumers are decoupled in Kafka: a handler that throws
      // (for example after a guard denies the message and no filter handles the
      // exception) must never fail the producer's send, so each delivery is
      // isolated and the transport's own error mapping decides commit-vs-retry.
      try {
        await consumer.eachMessage?.({
          topic,
          partition,
          message: toConsumed(messages[partition], partition),
        });
      } catch {
        // Swallowed deliberately: see comment above.
      }
    }
  }

  private async deliverBatch(
    consumer: ConsumerRegistration,
    topic: string,
    messages: KafkaProducerMessage[],
  ): Promise<void> {
    const byPartition = groupByPartition(messages);
    const deliveries = [...byPartition].map(([partition, partitionMessages]) =>
      consumer
        .eachBatch?.({
          batch: { topic, partition, messages: partitionMessages },
          resolveOffset: () => {},
        })
        // Isolate each partition's batch the same way as per-message delivery.
        .catch(() => {}),
    );
    await Promise.all(deliveries);
  }
}

function groupByPartition(
  messages: KafkaProducerMessage[],
): Map<number, KafkaConsumerMessage[]> {
  const byPartition = new Map<number, KafkaConsumerMessage[]>();
  for (const message of messages) {
    const partition = message.partition ?? 0;
    const existing = byPartition.get(partition) ?? [];
    existing.push(toConsumed(message, existing.length));
    byPartition.set(partition, existing);
  }
  return byPartition;
}

function toConsumed(
  message: KafkaProducerMessage,
  offset: number,
): KafkaConsumerMessage {
  return {
    key: message.key ?? null,
    value: message.value,
    headers: message.headers,
    offset: String(offset),
  };
}

function metadata(topic: string, partition: number): KafkaRecordMetadata {
  return {
    topicName: topic,
    partition,
    errorCode: 0,
    offset: String(partition),
  };
}
