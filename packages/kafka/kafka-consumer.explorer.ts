import {
  Inject,
  Injectable,
  Logger,
  OnApplicationBootstrap,
  OnApplicationShutdown,
  Type,
} from '@nestjs/common';
import { PARAMTYPES_METADATA } from '@nestjs/common/constants';
import {
  ApplicationConfig,
  MetadataScanner,
  ModuleRef,
  ModulesContainer,
  Reflector,
} from '@nestjs/core';
import { ContextIdFactory } from '@nestjs/core/helpers/context-id-factory';
import { STATIC_CONTEXT } from '@nestjs/core/injector/constants';
import {
  KAFKA_CONSUMER_METADATA,
  KAFKA_HANDLER_METADATA,
} from './constants';
import {
  KafkaClientDriver,
  KafkaConsumerConfig,
  KafkaConsumerRunConfig,
  KafkaDriverConsumer,
} from './driver';
import {
  KafkaHandlerContext,
  KafkaContextCreator,
  KafkaHandlerInvocation,
} from './kafka-context-creator';
import { createKafkaEnhancerRuntime } from './kafka-enhancer-runtime.factory';
import { KafkaDispatcher, KafkaRoutes } from './kafka-dispatcher';
import {
  KafkaConsumerMetadata,
  KafkaHandlerMetadata,
  KafkaModuleOptions,
  KafkaTopicPattern,
} from './interfaces';
import {
  defaultKafkaErrorMapper,
  KafkaErrorMapper,
} from './kafka-error-mapping';
import { Controller } from './kafka-params.resolver';
import { KafkaProducerService } from './kafka-producer.service';
import { KafkaReplyPublisher } from './kafka-reply-publisher';
import {
  KafkaRetryBackoff,
  KafkaRetryBackoffOptions,
  resolveRetryBackoff,
} from './kafka-retry-backoff';
import { resolveHeaderKeys } from './kafka-request-reply.protocol';
import { KAFKA_CLIENT_DRIVER, KAFKA_MODULE_OPTIONS } from './tokens';

/** Default partitions-consumed-concurrently: ordered, one partition at a time. */
const DEFAULT_CONCURRENCY = 1;

/**
 * The slice of Nest's `InstanceWrapper` the explorer relies on, captured locally
 * so the package does not depend on the wrapper's full internal type.
 */
interface InstanceWrapperLike {
  instance?: unknown;
  metatype?: unknown;
  id?: string;
  isDependencyTreeStatic?: () => boolean;
}

interface DiscoveredHandler {
  topic: KafkaTopicPattern;
  groupId?: string;
  batch: boolean;
  reply: boolean;
  concurrency: number;
  maxInFlight: number;
  run: (invocation: KafkaHandlerInvocation) => Promise<unknown>;
  /** Only used to name the offending method in bootstrap validation errors. */
  describe: string;
}

/**
 * Identifies the Kafka consumer a handler belongs to: its consumer group plus
 * its consumption mode. Per-message and batch handlers never share a consumer
 * because a Confluent consumer runs either `eachMessage` or `eachBatch`.
 */
type ConsumerKey = string;

/** One started consumer, with what graceful shutdown needs to stop it. */
interface RunningConsumer {
  consumer: KafkaDriverConsumer;
  dispatcher: KafkaDispatcher;
  /** The exact topics it subscribed to; its pattern topics come from the dispatcher. */
  topics: string[];
}

/**
 * Discovers `@KafkaConsumer` classes, wires their `@KafkaHandler` methods
 * through the Nest enhancer pipeline, and subscribes them to their topics on the
 * underlying driver.
 *
 * It is the bridge between Nest's dependency-injection container and the Kafka
 * transport: discovery happens once at application bootstrap, handlers are
 * grouped by consumer group (and consumption mode) so partitions balance the way
 * Confluent expects, and every consumed message is dispatched through
 * {@link KafkaContextCreator} so guards, interceptors, pipes, and filters run
 * before the handler — exactly as `@nestjs/microservices` does for the official
 * Kafka transport.
 *
 * Per-topic concurrency (`nestjs/nest#12703`) and backpressure (BRIEF §9) are
 * resolved per handler and applied through the per-consumer
 * {@link KafkaDispatcher}.
 *
 * @internal
 */
@Injectable()
export class KafkaConsumerExplorer
  implements OnApplicationBootstrap, OnApplicationShutdown
{
  private readonly logger = new Logger(KafkaConsumerExplorer.name);
  private readonly contextCreator: KafkaContextCreator;
  private readonly running: RunningConsumer[] = [];
  private readonly errorMapper: KafkaErrorMapper;
  private readonly replier: KafkaReplyPublisher;
  /** Resolved `retryBackoff`, or `undefined` when it is turned off. */
  private readonly retryBackoff: Required<KafkaRetryBackoffOptions> | undefined;

  constructor(
    private readonly metadataScanner: MetadataScanner,
    private readonly modulesContainer: ModulesContainer,
    private readonly reflector: Reflector,
    private readonly applicationConfig: ApplicationConfig,
    private readonly moduleRef: ModuleRef,
    @Inject(KAFKA_CLIENT_DRIVER) private readonly driver: KafkaClientDriver,
    @Inject(KAFKA_MODULE_OPTIONS) private readonly options: KafkaModuleOptions,
    producer: KafkaProducerService,
  ) {
    this.contextCreator = new KafkaContextCreator(
      createKafkaEnhancerRuntime(this.modulesContainer, this.applicationConfig),
    );
    this.errorMapper = this.options.errorMapper ?? defaultKafkaErrorMapper;
    this.retryBackoff = resolveRetryBackoff(this.options.retryBackoff);
    // A replier is configuration-free by design: it answers wherever the
    // request's headers say. Only the header *keys* are configurable, and they
    // default to the `@nestjs/microservices` ones, so a migrated handler
    // answers an un-migrated caller with no module options at all.
    this.replier = new KafkaReplyPublisher(
      producer,
      resolveHeaderKeys(this.options.requestReply?.headers),
    );
  }

  /**
   * Discover consumers, subscribe them, and start dispatching messages once the
   * application has finished bootstrapping (so request-scoped providers and
   * global enhancers are fully registered).
   */
  async onApplicationBootstrap(): Promise<void> {
    const handlers = this.discoverHandlers();
    if (handlers.length === 0) {
      return;
    }
    this.assertOneReplierPerTopic(handlers);
    await this.startConsumers(handlers);
  }

  /**
   * Graceful shutdown, in the order the constitution requires: stop accepting
   * newly delivered records — pause every consumer so the client stops handing
   * them over, and have the dispatcher hand back any that still arrive — then
   * drain the work already in flight so no handler is interrupted mid-message,
   * then disconnect every consumer. A record that was fetched but not handled
   * is never committed, so the next member of the group receives it.
   */
  async onApplicationShutdown(): Promise<void> {
    for (const { consumer, dispatcher, topics } of this.running) {
      this.pauseForShutdown(consumer, [...topics, ...dispatcher.patternTopics()]);
    }
    await Promise.all(this.running.map(({ dispatcher }) => dispatcher.drain()));
    await Promise.all(this.running.map(({ consumer }) => consumer.disconnect()));
    this.running.length = 0;
  }

  /**
   * Best effort by design: if the driver cannot pause (or pausing fails, for a
   * consumer that never connected), shutdown carries on — the dispatcher still
   * rejects every late record, so nothing is committed that was not handled.
   */
  private pauseForShutdown(consumer: KafkaDriverConsumer, topics: string[]): void {
    if (!consumer.pause) {
      return;
    }
    try {
      consumer.pause(topics.map(topic => ({ topic })));
    } catch (error) {
      this.logger.warn(
        `Could not pause a consumer before draining (${describeError(error)}); ` +
          'records delivered during the drain are handed back instead.',
      );
    }
  }

  private discoverHandlers(): DiscoveredHandler[] {
    const handlers: DiscoveredHandler[] = [];
    // Iterate the modules so the owning module key — needed by the Nest enhancer
    // creators — comes straight from the container, no reverse lookup required.
    for (const [moduleKey, moduleRef] of this.modulesContainer) {
      for (const wrapper of moduleRef.providers.values()) {
        this.collectFromProvider(wrapper, moduleKey, handlers);
      }
    }
    return handlers;
  }

  private collectFromProvider(
    wrapper: InstanceWrapperLike,
    moduleKey: string,
    handlers: DiscoveredHandler[],
  ): void {
    const { instance, metatype } = wrapper;
    if (!instance || !metatype) {
      return;
    }

    const consumerMeta: KafkaConsumerMetadata | undefined = this.reflector.get(
      KAFKA_CONSUMER_METADATA,
      metatype as Type,
    );
    if (!consumerMeta) {
      return;
    }

    const prototype = Object.getPrototypeOf(instance);
    for (const methodName of this.metadataScanner.getAllMethodNames(prototype)) {
      this.collectFromMethod({
        wrapper,
        metatype: metatype as Type,
        prototype,
        methodName,
        consumerMeta,
        moduleKey,
        handlers,
      });
    }
  }

  private collectFromMethod(params: {
    wrapper: InstanceWrapperLike;
    metatype: Type;
    prototype: Record<string, unknown>;
    methodName: string;
    consumerMeta: KafkaConsumerMetadata;
    moduleKey: string;
    handlers: DiscoveredHandler[];
  }): void {
    const methodRef = params.prototype[params.methodName] as (
      ...args: unknown[]
    ) => unknown;

    const handlerMeta: KafkaHandlerMetadata | undefined = Reflect.getMetadata(
      KAFKA_HANDLER_METADATA,
      methodRef,
    );
    if (!handlerMeta) {
      return;
    }

    const topic = handlerMeta.topic ?? params.consumerMeta.topic;
    if (!topic) {
      throw this.missingTopicError(params.metatype.name, params.methodName);
    }

    const describe = `${params.metatype.name}.${params.methodName}`;
    const batch = handlerMeta.options.batch ?? false;
    const reply = handlerMeta.options.reply ?? false;
    if (batch && reply) {
      throw this.batchReplyError(describe);
    }
    if (topic instanceof RegExp) {
      this.assertUsablePattern(topic, reply, describe);
    }

    params.handlers.push({
      topic,
      describe,
      groupId:
        handlerMeta.options.groupId ?? params.consumerMeta.options.groupId,
      batch,
      reply,
      concurrency: this.resolve(
        'concurrency',
        handlerMeta,
        params.consumerMeta,
        DEFAULT_CONCURRENCY,
      ),
      maxInFlight: this.resolve(
        'maxInFlight',
        handlerMeta,
        params.consumerMeta,
        0,
      ),
      run: this.createRunner({
        wrapper: params.wrapper,
        metatype: params.metatype,
        prototype: params.prototype,
        methodName: params.methodName,
        methodRef,
        moduleKey: params.moduleKey,
      }),
    });

    this.logger.log(
      `Mapped "${topic}" to ${params.metatype.name}.${params.methodName}`,
    );
  }

  /**
   * Resolve a numeric concurrency/backpressure option handler → consumer →
   * module default, so a single handler can opt out of (or into) the wider
   * setting.
   */
  private resolve(
    key: 'concurrency' | 'maxInFlight',
    handlerMeta: KafkaHandlerMetadata,
    consumerMeta: KafkaConsumerMetadata,
    fallback: number,
  ): number {
    return (
      handlerMeta.options[key] ??
      consumerMeta.options[key] ??
      this.options[key] ??
      fallback
    );
  }

  private createRunner(params: {
    wrapper: InstanceWrapperLike;
    metatype: Type;
    prototype: Record<string, unknown>;
    methodName: string;
    methodRef: (...args: unknown[]) => unknown;
    moduleKey: string;
  }): (invocation: KafkaHandlerInvocation) => Promise<unknown> {
    const paramTypes: unknown[] =
      Reflect.getMetadata(
        PARAMTYPES_METADATA,
        params.prototype,
        params.methodName,
      ) ?? [];

    // Static (default/singleton) consumers reuse one instance; request-scoped
    // consumers get a fresh context — and therefore a fresh instance — for every
    // consumed message, exactly as `@nestjs/microservices` resolves them.
    const isStatic = params.wrapper.isDependencyTreeStatic?.() ?? true;

    const handler: KafkaHandlerContext = {
      callback: params.methodRef,
      metatype: params.metatype,
      methodName: params.methodName,
      moduleKey: params.moduleKey,
      paramTypes,
      inquirerId: params.wrapper.id,
      resolveContextId: () =>
        isStatic ? STATIC_CONTEXT : ContextIdFactory.create(),
      resolveInstance: contextId =>
        this.resolveInstance(params.wrapper, params.metatype, contextId),
    };

    return this.contextCreator.create(handler);
  }

  private async resolveInstance(
    wrapper: InstanceWrapperLike,
    metatype: Type,
    contextId: { id: number },
  ): Promise<Controller> {
    if (contextId === STATIC_CONTEXT && wrapper.instance) {
      return wrapper.instance as Controller;
    }

    return (await this.moduleRef.resolve(metatype, contextId, {
      strict: false,
    })) as Controller;
  }

  private async startConsumers(handlers: DiscoveredHandler[]): Promise<void> {
    const groups = this.groupByConsumer(handlers);
    for (const groupHandlers of groups.values()) {
      await this.startConsumer(groupHandlers);
    }
  }

  /**
   * Group handlers by `(groupId, consumption mode)`. Per-message and batch
   * handlers in the same group still land on separate consumers because a single
   * Confluent consumer runs either `eachMessage` or `eachBatch`.
   */
  private groupByConsumer(
    handlers: DiscoveredHandler[],
  ): Map<ConsumerKey, DiscoveredHandler[]> {
    const groups = new Map<ConsumerKey, DiscoveredHandler[]>();
    for (const handler of handlers) {
      const key: ConsumerKey = `${handler.groupId ?? ''}|${handler.batch}`;
      const existing = groups.get(key);
      if (existing) {
        existing.push(handler);
      } else {
        groups.set(key, [handler]);
      }
    }
    return groups;
  }

  private async startConsumer(handlers: DiscoveredHandler[]): Promise<void> {
    const routes = buildRoutes(handlers);
    const [first] = handlers;
    const config: KafkaConsumerConfig = {};
    if (first.groupId !== undefined) {
      config.groupId = first.groupId;
    }

    const consumer = this.driver.createConsumer(config);
    const maxInFlight = Math.max(...handlers.map(handler => handler.maxInFlight));
    const dispatcher = new KafkaDispatcher(
      routes,
      this.errorMapper,
      maxInFlight,
      this.replier,
      this.createBackoff(consumer),
    );
    const topics = [...routes.topics.keys()];
    this.running.push({ consumer, dispatcher, topics });

    await consumer.connect();
    await consumer.subscribe({
      topics: [...topics, ...routes.patterns.map(({ pattern }) => pattern)],
    });
    await consumer.run(this.runConfig(first, dispatcher, handlers));
  }

  /**
   * The retry backoff for one consumer, or `undefined` when it is turned off or
   * the driver cannot pause and resume partitions — retries then redeliver
   * immediately, as the client does on its own.
   */
  private createBackoff(
    consumer: KafkaDriverConsumer,
  ): KafkaRetryBackoff | undefined {
    const { pause, resume } = consumer;
    if (!this.retryBackoff || !pause || !resume) {
      return undefined;
    }
    return new KafkaRetryBackoff(this.retryBackoff, {
      pause: topics => pause.call(consumer, topics),
      resume: topics => resume.call(consumer, topics),
    });
  }

  /**
   * Build the `run` config for a consumer: `eachBatch` for batch handlers,
   * `eachMessage` otherwise, plus the partition concurrency
   * (`partitionsConsumedConcurrently`, the documented opt-out of sequential
   * per-topic processing — `nestjs/nest#12703`).
   */
  private runConfig(
    first: DiscoveredHandler,
    dispatcher: KafkaDispatcher,
    handlers: DiscoveredHandler[],
  ): KafkaConsumerRunConfig {
    const partitionsConsumedConcurrently = Math.max(
      ...handlers.map(handler => handler.concurrency),
    );
    if (first.batch) {
      return {
        partitionsConsumedConcurrently,
        // The dispatcher resolves a batch's offsets itself, once its handlers
        // settled without a 'retry' — the rule lives in this package rather
        // than in one client's auto-resolve, so every driver gets it.
        eachBatchAutoResolve: false,
        eachBatch: payload => dispatcher.eachBatch(payload),
      };
    }
    return {
      partitionsConsumedConcurrently,
      eachMessage: payload => dispatcher.eachMessage(payload),
    };
  }

  /**
   * Two replying handlers on one topic means two replies per request; the second
   * one always loses the correlation race and is dropped. Refusing to start
   * beats shipping that heisenbug.
   */
  private assertOneReplierPerTopic(handlers: DiscoveredHandler[]): void {
    const repliers = new Map<string, string>();
    for (const handler of handlers) {
      // A replier always names its topic: patterns are refused for it at
      // discovery, so every topic here is a string.
      if (!handler.reply || typeof handler.topic !== 'string') {
        continue;
      }
      const existing = repliers.get(handler.topic);
      if (existing !== undefined) {
        throw this.duplicateReplierError(handler.topic, existing, handler.describe);
      }
      repliers.set(handler.topic, handler.describe);
    }
  }

  /**
   * Confluent's client accepts a pattern only in the form `librdkafka`
   * understands — anchored with `^`, no flags — and a replier must name its
   * topic: with patterns, two repliers could match one request topic and answer
   * it twice, which the one-replier-per-topic check could no longer see.
   */
  private assertUsablePattern(
    pattern: RegExp,
    reply: boolean,
    handler: string,
  ): void {
    if (!pattern.source.startsWith('^') || pattern.flags !== '') {
      throw new Error(
        `Kafka handler ${handler} subscribes to ${String(pattern)}, but a topic ` +
          'pattern must start with "^" and carry no flags — the form ' +
          "Confluent's client hands to librdkafka. Anchor it, e.g. " +
          '/^orders\\..*/.',
      );
    }
    if (JS_ONLY_PATTERN_SYNTAX.test(pattern.source)) {
      throw new Error(
        `Kafka handler ${handler} subscribes to ${String(pattern)}, which uses ` +
          'syntax only JavaScript understands. librdkafka matches the ' +
          'subscription with POSIX extended regular expressions, which read ' +
          '\\d as a literal "d" and reject (?:…) and lookarounds — so the ' +
          'broker would subscribe to other topics than this package routes. ' +
          'Use [0-9], [A-Za-z0-9_], and plain groups instead.',
      );
    }
    if (reply) {
      throw new Error(
        `Kafka handler ${handler} declares "reply: true" on the pattern ` +
          `${String(pattern)}. A replying handler must name its request topic: ` +
          'with patterns, two repliers could match one topic and answer every ' +
          'request twice.',
      );
    }
  }

  private missingTopicError(className: string, methodName: string): Error {
    return new Error(
      `Kafka handler ${className}.${methodName} has no topic. Pass a topic to ` +
        '@KafkaHandler("topic") or set a default topic on @KafkaConsumer("topic").',
    );
  }

  private batchReplyError(handler: string): Error {
    return new Error(
      `Kafka handler ${handler} declares both "batch: true" and ` +
        '"reply: true". A batch has no single request to answer, so the ' +
        'combination is not supported — drop one of the two flags.',
    );
  }

  private duplicateReplierError(
    topic: string,
    first: string,
    second: string,
  ): Error {
    return new Error(
      `Kafka handlers ${first} and ${second} both declare "reply: true" for ` +
        `topic "${topic}". Two repliers answer every request twice and the ` +
        'second reply always loses the correlation race — route exactly one ' +
        'replying handler per topic.',
    );
  }
}

function describeError(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

/**
 * Regex syntax JavaScript understands but librdkafka's POSIX engine does not:
 * `(?` (non-capturing groups, lookarounds, named groups) and the backslash
 * classes. The pattern is matched twice — by `librdkafka` for the subscription,
 * by JavaScript for routing — so both must read it the same way.
 */
const JS_ONLY_PATTERN_SYNTAX = /\(\?|\\[dDwWsSbB]/;

/**
 * Group handlers into the dispatcher's routes: by exact topic, and by pattern —
 * handlers sharing a pattern's source share one route, as handlers sharing a
 * topic do.
 */
function buildRoutes(handlers: DiscoveredHandler[]): KafkaRoutes {
  const topics = new Map<string, DiscoveredHandler[]>();
  const patterns = new Map<string, { pattern: RegExp; handlers: DiscoveredHandler[] }>();
  for (const handler of handlers) {
    const { topic } = handler;
    if (typeof topic === 'string') {
      topics.set(topic, [...(topics.get(topic) ?? []), handler]);
      continue;
    }
    const route = patterns.get(topic.source) ?? { pattern: topic, handlers: [] };
    route.handlers.push(handler);
    patterns.set(topic.source, route);
  }
  return { topics, patterns: [...patterns.values()] };
}
