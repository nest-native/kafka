import { ContextType, ForbiddenException, PipeTransform } from '@nestjs/common';
import { RouteParamtypes } from '@nestjs/common/enums/route-paramtypes.enum';
import { Controller } from '@nestjs/common/interfaces';
import { ExecutionContextHost } from '@nestjs/core/helpers/execution-context-host';
import { STATIC_CONTEXT } from '@nestjs/core/injector/constants';
import { isObservable, lastValueFrom } from 'rxjs';
import { KafkaBatchContext, KafkaContext } from './kafka-context';
import {
  KafkaParamsPipes,
  KafkaParamsResolver,
} from './kafka-params.resolver';

/**
 * The Kafka transport runs handlers under Nest's `'rpc'` execution-context type,
 * matching `@nestjs/microservices`. Guards, interceptors, and filters that
 * branch on the context type therefore behave exactly as they do for the
 * official Kafka transport.
 */
const KAFKA_CONTEXT_TYPE: ContextType = 'rpc';

type KafkaCallback = (...args: unknown[]) => unknown;
type KafkaContextId = { id: number };

interface KafkaGuardsContextCreatorLike {
  create(
    instance: Controller,
    callback: KafkaCallback,
    moduleKey: string,
    contextId: KafkaContextId,
    inquirerId?: string,
  ): unknown[];
}

interface KafkaGuardsConsumerLike {
  tryActivate(
    guards: unknown[],
    args: unknown[],
    instance: Controller,
    callback: KafkaCallback,
    type: ContextType,
  ): Promise<boolean>;
}

interface KafkaInterceptorsContextCreatorLike {
  create(
    instance: Controller,
    callback: KafkaCallback,
    moduleKey: string,
    contextId: KafkaContextId,
    inquirerId?: string,
  ): unknown[];
}

interface KafkaInterceptorsConsumerLike {
  intercept(
    interceptors: unknown[],
    args: unknown[],
    instance: Controller,
    callback: KafkaCallback,
    next: () => Promise<unknown>,
    type: ContextType,
  ): Promise<unknown>;
}

interface KafkaPipesContextCreatorLike {
  create(
    instance: Controller,
    callback: KafkaCallback,
    moduleKey: string,
    contextId: KafkaContextId,
    inquirerId?: string,
  ): PipeTransform[];
  createConcreteContext(
    metadata: unknown[],
    contextId?: KafkaContextId,
    inquirerId?: string,
  ): PipeTransform[];
  setModuleContext(moduleKey: string): void;
}

interface KafkaPipesConsumerLike {
  apply(
    value: unknown,
    metadata: { type: unknown; metatype?: unknown; data?: string },
    pipes: PipeTransform[],
  ): Promise<unknown>;
}

interface KafkaExceptionHandlerLike {
  next(error: Error, executionContext: ExecutionContextHost): unknown;
}

interface KafkaExceptionFiltersContextLike {
  create(
    instance: Controller,
    callback: KafkaCallback,
    moduleKey: string,
    contextId: KafkaContextId,
    inquirerId?: string,
  ): KafkaExceptionHandlerLike;
}

/**
 * The Nest enhancer building blocks the context creator orchestrates. Built by
 * {@link createKafkaEnhancerRuntime}.
 */
export interface KafkaEnhancerRuntime {
  guardsContextCreator: KafkaGuardsContextCreatorLike;
  guardsConsumer: KafkaGuardsConsumerLike;
  interceptorsContextCreator: KafkaInterceptorsContextCreatorLike;
  interceptorsConsumer: KafkaInterceptorsConsumerLike;
  pipesContextCreator: KafkaPipesContextCreatorLike;
  pipesConsumer: KafkaPipesConsumerLike;
  exceptionFiltersContext: KafkaExceptionFiltersContextLike;
}

/**
 * Everything the context creator needs to build one handler runner. Mirrors the
 * shape the explorer assembles per discovered `@KafkaHandler`.
 */
export interface KafkaHandlerContext {
  callback: KafkaCallback;
  /**
   * The consumer class the handler lives on. Used to reflect the
   * parameter-decorator metadata once per handler.
   */
  metatype: Controller['constructor'];
  methodName: string;
  moduleKey: string;
  paramTypes: unknown[];
  inquirerId?: string;
  resolveContextId: () => KafkaContextId;
  resolveInstance: (contextId: KafkaContextId) => Promise<Controller>;
}

/**
 * The payload a built handler runner receives for every consumed message (or, for
 * a batch handler, every fetched batch). `payload` is the deserialized message
 * for a per-message handler and the array of deserialized messages for a batch
 * handler; `context` is the matching {@link KafkaContext} or
 * {@link KafkaBatchContext}.
 */
export interface KafkaHandlerInvocation {
  payload: unknown;
  context: KafkaContext | KafkaBatchContext;
}

/**
 * Wraps `@KafkaHandler` methods so each consumed message flows through the full
 * Nest enhancer pipeline — guards, interceptors, pipes, and exception filters —
 * before the handler runs, exactly as `@nestjs/microservices` does for the
 * official Kafka transport. This is the piece that makes `@UseGuards`,
 * `@UseInterceptors`, `@UsePipes`, and `@UseFilters` work on consumer methods.
 *
 * @internal
 */
export class KafkaContextCreator {
  constructor(private readonly runtime: KafkaEnhancerRuntime) {}

  /**
   * Build the per-message runner for a single handler. The returned function is
   * called once per consumed message; it resolves the (possibly request-scoped)
   * instance, runs the enhancer pipeline, and invokes the handler.
   */
  create(
    handler: KafkaHandlerContext,
  ): (invocation: KafkaHandlerInvocation) => Promise<unknown> {
    // Parameter-decorator metadata is static on the class, so reflect it once
    // when the handler is wired rather than per consumed message.
    const paramsResolver = this.createParamsResolver(handler);

    return async (invocation: KafkaHandlerInvocation) => {
      const contextId = handler.resolveContextId();
      const instance = await handler.resolveInstance(contextId);
      const callback = this.resolveCallback(instance, handler);

      const contextArgs = [invocation.payload, invocation.context];
      const executionContext = this.createExecutionContext(
        instance,
        callback,
        contextArgs,
      );
      const exceptionHandler = this.runtime.exceptionFiltersContext.create(
        instance,
        callback,
        handler.moduleKey,
        contextId,
        handler.inquirerId,
      );

      try {
        return await this.run(handler, instance, callback, contextId, {
          executionContext,
          paramsResolver,
        });
      } catch (error) {
        return this.handleException(error, exceptionHandler, executionContext);
      }
    };
  }

  private createParamsResolver(
    handler: KafkaHandlerContext,
  ): KafkaParamsResolver {
    const pipesRuntime: KafkaParamsPipes = {
      createConcrete: pipes =>
        this.runtime.pipesContextCreator.createConcreteContext(pipes),
      apply: (value, index, metatype, pipes) =>
        this.runtime.pipesConsumer.apply(
          value,
          {
            type: RouteParamtypes.BODY as unknown,
            metatype,
            data: undefined,
          },
          pipes,
        ),
    };
    return new KafkaParamsResolver(
      handler.metatype,
      handler.methodName,
      handler.paramTypes,
      pipesRuntime,
    );
  }

  private resolveCallback(
    instance: Controller,
    handler: KafkaHandlerContext,
  ): KafkaCallback {
    const candidate = (instance as Record<string, unknown>)[handler.methodName];
    return typeof candidate === 'function'
      ? (candidate as KafkaCallback)
      : handler.callback;
  }

  private createExecutionContext(
    instance: Controller,
    callback: KafkaCallback,
    contextArgs: unknown[],
  ): ExecutionContextHost {
    const executionContext = new ExecutionContextHost(
      contextArgs,
      instance.constructor as never,
      callback as never,
    );
    executionContext.setType(KAFKA_CONTEXT_TYPE);
    return executionContext;
  }

  private async run(
    handler: KafkaHandlerContext,
    instance: Controller,
    callback: KafkaCallback,
    contextId: KafkaContextId,
    state: {
      executionContext: ExecutionContextHost;
      paramsResolver: KafkaParamsResolver;
    },
  ): Promise<unknown> {
    const contextArgs = state.executionContext.getArgs() as unknown[];
    const guards = this.runtime.guardsContextCreator.create(
      instance,
      callback,
      handler.moduleKey,
      contextId,
      handler.inquirerId,
    );
    await this.activateGuards(guards, contextArgs, instance, callback);

    const interceptors = this.runtime.interceptorsContextCreator.create(
      instance,
      callback,
      handler.moduleKey,
      contextId,
      handler.inquirerId,
    );
    const pipes = this.runtime.pipesContextCreator.create(
      instance,
      callback,
      handler.moduleKey,
      contextId,
      handler.inquirerId,
    );
    // Param-level pipes resolve against the owning module, mirroring how
    // `@nestjs/microservices`'s RpcContextCreator scopes custom-param pipes.
    this.runtime.pipesContextCreator.setModuleContext(handler.moduleKey);

    const invoke = this.createInvoker(handler, instance, callback, pipes, state);

    const result =
      interceptors.length === 0
        ? await invoke()
        : await this.runtime.interceptorsConsumer.intercept(
            interceptors,
            contextArgs,
            instance,
            callback,
            invoke,
            KAFKA_CONTEXT_TYPE,
          );
    return this.transformResult(result);
  }

  private async activateGuards(
    guards: unknown[],
    contextArgs: unknown[],
    instance: Controller,
    callback: KafkaCallback,
  ): Promise<void> {
    if (guards.length === 0) {
      return;
    }

    const canActivate = await this.runtime.guardsConsumer.tryActivate(
      guards,
      contextArgs,
      instance,
      callback,
      KAFKA_CONTEXT_TYPE,
    );
    if (!canActivate) {
      throw new ForbiddenException();
    }
  }

  private createInvoker(
    handler: KafkaHandlerContext,
    instance: Controller,
    callback: KafkaCallback,
    pipes: PipeTransform[],
    state: {
      executionContext: ExecutionContextHost;
      paramsResolver: KafkaParamsResolver;
    },
  ): () => Promise<unknown> {
    if (state.paramsResolver.hasParams()) {
      return this.createParamInvoker(handler, instance, callback, pipes, state);
    }
    return this.createPositionalInvoker(handler, instance, callback, pipes, state);
  }

  /**
   * Invoke a handler that declares parameter decorators. The args array is built
   * entirely from the `@KafkaMessage()` / `@KafkaHeaders()` / `@KafkaCtx()`
   * factories and their param pipes; method-level pipes (`@UsePipes`) then run
   * over the resolved payload argument so both pipe tiers apply.
   */
  private createParamInvoker(
    handler: KafkaHandlerContext,
    instance: Controller,
    callback: KafkaCallback,
    pipes: PipeTransform[],
    state: {
      executionContext: ExecutionContextHost;
      paramsResolver: KafkaParamsResolver;
    },
  ): () => Promise<unknown> {
    return async () => {
      const args = await state.paramsResolver.resolve(state.executionContext);
      if (pipes.length > 0) {
        args[0] = await this.applyPayloadPipes(args[0], pipes, handler.paramTypes);
      }
      return callback.apply(instance, args);
    };
  }

  /**
   * Invoke a handler that takes the positional `(payload, context)` arguments —
   * the convention for handlers written without parameter decorators. Method
   * pipes run over the payload, exactly as before parameter decorators existed.
   */
  private createPositionalInvoker(
    handler: KafkaHandlerContext,
    instance: Controller,
    callback: KafkaCallback,
    pipes: PipeTransform[],
    state: { executionContext: ExecutionContextHost },
  ): () => Promise<unknown> {
    return async () => {
      const args = [...(state.executionContext.getArgs() as unknown[])];
      if (pipes.length > 0) {
        args[0] = await this.applyPayloadPipes(args[0], pipes, handler.paramTypes);
      }
      return callback.apply(instance, args);
    };
  }

  private applyPayloadPipes(
    payload: unknown,
    pipes: PipeTransform[],
    paramTypes: unknown[],
  ): Promise<unknown> {
    return this.runtime.pipesConsumer.apply(
      payload,
      {
        type: RouteParamtypes.BODY as unknown,
        metatype: paramTypes[0],
        data: undefined,
      },
      pipes,
    );
  }

  private async handleException(
    error: unknown,
    exceptionHandler: KafkaExceptionHandlerLike,
    executionContext: ExecutionContextHost,
  ): Promise<unknown> {
    const handled = await exceptionHandler.next(
      error as Error,
      executionContext,
    );
    return this.transformResult(handled);
  }

  private async transformResult(result: unknown): Promise<unknown> {
    if (isObservable(result)) {
      return lastValueFrom(result);
    }
    return result;
  }
}
