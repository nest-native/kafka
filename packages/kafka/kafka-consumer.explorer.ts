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
  KafkaDriverConsumer,
  KafkaEachMessagePayload,
} from './driver';
import {
  KafkaHandlerContext,
  KafkaContextCreator,
  KafkaHandlerInvocation,
} from './kafka-context-creator';
import { createKafkaEnhancerRuntime } from './kafka-enhancer-runtime.factory';
import { KafkaContext, KafkaIncomingMessage } from './kafka-context';
import {
  KafkaConsumerMetadata,
  KafkaHandlerMetadata,
  KafkaModuleOptions,
} from './interfaces';
import {
  applyKafkaErrorBehavior,
  defaultKafkaErrorMapper,
  KafkaErrorMapper,
} from './kafka-error-mapping';
import { KAFKA_CLIENT_DRIVER, KAFKA_MODULE_OPTIONS } from './tokens';

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
  run: (invocation: KafkaHandlerInvocation) => Promise<unknown>;
}

/**
 * Discovers `@KafkaConsumer` classes, wires their `@KafkaHandler` methods
 * through the Nest enhancer pipeline, and subscribes them to their topics on the
 * underlying driver.
 *
 * It is the bridge between Nest's dependency-injection container and the Kafka
 * transport: discovery happens once at application bootstrap, handlers are
 * grouped by consumer group so partitions balance the way Confluent expects, and
 * every consumed message is dispatched through {@link KafkaContextCreator} so
 * guards, interceptors, pipes, and filters run before the handler — exactly as
 * `@nestjs/microservices` does for the official Kafka transport.
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
  private readonly errorMapper: KafkaErrorMapper;

  /**
   * Messages currently being handled. Graceful shutdown drains this set before
   * disconnecting so an in-flight handler is never interrupted mid-message.
   */
  private readonly inFlight = new Set<Promise<void>>();

  /**
   * Once shutdown begins the explorer stops accepting newly delivered messages
   * (§ graceful shutdown: stop new claims → drain in-flight → disconnect).
   */
  private shuttingDown = false;

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
   * newly delivered messages, drain the messages already in flight so no handler
   * is interrupted mid-message, then disconnect every consumer.
   */
  async onApplicationShutdown(): Promise<void> {
    this.shuttingDown = true;
    await this.drainInFlight();
    await Promise.all(this.consumers.map(consumer => consumer.disconnect()));
    this.consumers.length = 0;
  }

  /**
   * Wait for every in-flight message to settle. A draining handler may itself
   * dispatch nothing new (new claims are already refused), so a single pass over
   * the snapshot is enough.
   */
  private async drainInFlight(): Promise<void> {
    await Promise.allSettled([...this.inFlight]);
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
    const groups = this.groupByConsumerGroup(handlers);
    for (const [groupId, groupHandlers] of groups) {
      await this.startConsumer(groupId, groupHandlers);
    }
  }

  private groupByConsumerGroup(
    handlers: DiscoveredHandler[],
  ): Map<string | undefined, DiscoveredHandler[]> {
    const groups = new Map<string | undefined, DiscoveredHandler[]>();
    for (const handler of handlers) {
      const existing = groups.get(handler.groupId);
      if (existing) {
        existing.push(handler);
      } else {
        groups.set(handler.groupId, [handler]);
      }
    }
    return groups;
  }

  private async startConsumer(
    groupId: string | undefined,
    handlers: DiscoveredHandler[],
  ): Promise<void> {
    const routes = this.buildRoutes(handlers);
    const config: KafkaConsumerConfig = {};
    if (groupId !== undefined) {
      config.groupId = groupId;
    }

    const consumer = this.driver.createConsumer(config);
    this.consumers.push(consumer);

    await consumer.connect();
    await consumer.subscribe({ topics: [...routes.keys()] });
    await consumer.run({
      eachMessage: payload => this.dispatch(routes, payload),
    });
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

  private dispatch(
    routes: Map<string, DiscoveredHandler[]>,
    payload: KafkaEachMessagePayload,
  ): Promise<void> {
    // Stop accepting new claims once shutdown has begun: a message delivered
    // during drain is ignored so its offset stays uncommitted and the broker
    // redelivers it to the next instance instead of being dropped here.
    if (this.shuttingDown) {
      return Promise.resolve();
    }

    const matched = routes.get(payload.topic);
    if (!matched) {
      return Promise.resolve();
    }

    const work = this.runHandlers(matched, payload);
    this.track(work);
    return work;
  }

  /**
   * Track an in-flight message so graceful shutdown can drain it. The promise is
   * removed once it settles, whether it resolved or rejected.
   */
  private track(work: Promise<void>): void {
    this.inFlight.add(work);
    const forget = (): void => {
      this.inFlight.delete(work);
    };
    work.then(forget, forget);
  }

  private async runHandlers(
    matched: DiscoveredHandler[],
    payload: KafkaEachMessagePayload,
  ): Promise<void> {
    const invocation = this.toInvocation(payload);
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

  private toInvocation(payload: KafkaEachMessagePayload): KafkaHandlerInvocation {
    const message: KafkaIncomingMessage = payload.message;
    const context = new KafkaContext(
      payload.topic,
      payload.partition,
      message,
    );
    return { payload: this.deserialize(message.value), context };
  }

  /**
   * Decode the message value into a handler payload. Buffers and strings are
   * JSON-parsed when they hold JSON, mirroring the default deserializer of the
   * official Kafka transport; non-JSON values pass through as the decoded
   * string. `null` (a tombstone) passes through unchanged.
   */
  private deserialize(value: KafkaIncomingMessage['value']): unknown {
    if (value === null) {
      return null;
    }
    const text = Buffer.isBuffer(value) ? value.toString('utf8') : value;
    try {
      return JSON.parse(text);
    } catch {
      return text;
    }
  }

  private missingTopicError(className: string, methodName: string): Error {
    return new Error(
      `Kafka handler ${className}.${methodName} has no topic. Pass a topic to ` +
        '@KafkaHandler("topic") or set a default topic on @KafkaConsumer("topic").',
    );
  }
}
