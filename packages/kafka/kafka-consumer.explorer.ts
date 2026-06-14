import {
  Inject,
  Injectable,
  Logger,
  OnApplicationBootstrap,
  OnApplicationShutdown,
  Type,
} from '@nestjs/common';
import { PARAMTYPES_METADATA } from '@nestjs/common/constants';
import { Controller } from '@nestjs/common/interfaces';
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
import { KafkaDispatcher } from './kafka-dispatcher';
import {
  KafkaConsumerMetadata,
  KafkaHandlerMetadata,
  KafkaModuleOptions,
} from './interfaces';
import {
  defaultKafkaErrorMapper,
  KafkaErrorMapper,
} from './kafka-error-mapping';
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
  topic: string;
  groupId?: string;
  batch: boolean;
  concurrency: number;
  maxInFlight: number;
  run: (invocation: KafkaHandlerInvocation) => Promise<unknown>;
}

/**
 * Identifies the Kafka consumer a handler belongs to: its consumer group plus
 * its consumption mode. Per-message and batch handlers never share a consumer
 * because a Confluent consumer runs either `eachMessage` or `eachBatch`.
 */
type ConsumerKey = string;

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
  private readonly consumers: KafkaDriverConsumer[] = [];
  private readonly dispatchers: KafkaDispatcher[] = [];
  private readonly errorMapper: KafkaErrorMapper;

  constructor(
    private readonly metadataScanner: MetadataScanner,
    private readonly modulesContainer: ModulesContainer,
    private readonly reflector: Reflector,
    private readonly applicationConfig: ApplicationConfig,
    private readonly moduleRef: ModuleRef,
    @Inject(KAFKA_CLIENT_DRIVER) private readonly driver: KafkaClientDriver,
    @Inject(KAFKA_MODULE_OPTIONS) private readonly options: KafkaModuleOptions,
  ) {
    this.contextCreator = new KafkaContextCreator(
      createKafkaEnhancerRuntime(this.modulesContainer, this.applicationConfig),
    );
    this.errorMapper = this.options.errorMapper ?? defaultKafkaErrorMapper;
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
    await this.startConsumers(handlers);
  }

  /**
   * Graceful shutdown, in the order the constitution requires: stop accepting
   * newly delivered records and drain the work already in flight so no handler is
   * interrupted mid-message, then disconnect every consumer.
   */
  async onApplicationShutdown(): Promise<void> {
    await Promise.all(this.dispatchers.map(dispatcher => dispatcher.drain()));
    await Promise.all(this.consumers.map(consumer => consumer.disconnect()));
    this.consumers.length = 0;
    this.dispatchers.length = 0;
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

    params.handlers.push({
      topic,
      groupId:
        handlerMeta.options.groupId ?? params.consumerMeta.options.groupId,
      batch: handlerMeta.options.batch ?? false,
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
    const routes = this.buildRoutes(handlers);
    const [first] = handlers;
    const config: KafkaConsumerConfig = {};
    if (first.groupId !== undefined) {
      config.groupId = first.groupId;
    }

    const consumer = this.driver.createConsumer(config);
    this.consumers.push(consumer);

    const maxInFlight = Math.max(...handlers.map(handler => handler.maxInFlight));
    const dispatcher = new KafkaDispatcher(routes, this.errorMapper, maxInFlight);
    this.dispatchers.push(dispatcher);

    await consumer.connect();
    await consumer.subscribe({ topics: [...routes.keys()] });
    await consumer.run(this.runConfig(first, dispatcher, handlers));
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
        // Offsets resolve per message inside the dispatcher, so disable the
        // client's all-or-nothing auto-resolve for rebalance safety.
        eachBatchAutoResolve: false,
        eachBatch: payload => dispatcher.eachBatch(payload),
      };
    }
    return {
      partitionsConsumedConcurrently,
      eachMessage: payload => dispatcher.eachMessage(payload),
    };
  }

  private buildRoutes(
    handlers: DiscoveredHandler[],
  ): Map<string, DiscoveredHandler[]> {
    const routes = new Map<string, DiscoveredHandler[]>();
    for (const handler of handlers) {
      const existing = routes.get(handler.topic);
      if (existing) {
        existing.push(handler);
      } else {
        routes.set(handler.topic, [handler]);
      }
    }
    return routes;
  }

  private missingTopicError(className: string, methodName: string): Error {
    return new Error(
      `Kafka handler ${className}.${methodName} has no topic. Pass a topic to ` +
        '@KafkaHandler("topic") or set a default topic on @KafkaConsumer("topic").',
    );
  }
}
