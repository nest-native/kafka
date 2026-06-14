import { ContextType, ForbiddenException, PipeTransform } from '@nestjs/common';
import { RouteParamtypes } from '@nestjs/common/enums/route-paramtypes.enum';
import { Controller } from '@nestjs/common/interfaces';
import { ExecutionContextHost } from '@nestjs/core/helpers/execution-context-host';
import { STATIC_CONTEXT } from '@nestjs/core/injector/constants';
import { isObservable, lastValueFrom } from 'rxjs';
import { KafkaContext } from './kafka-context';

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
  methodName: string;
  moduleKey: string;
  paramTypes: unknown[];
  inquirerId?: string;
  resolveContextId: () => KafkaContextId;
  resolveInstance: (contextId: KafkaContextId) => Promise<Controller>;
}

/**
 * The payload a built handler runner receives for every consumed message.
 */
export interface KafkaHandlerInvocation {
  payload: unknown;
  context: KafkaContext;
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
          contextArgs,
          payload: invocation.payload,
        });
      } catch (error) {
        return this.handleException(error, exceptionHandler, executionContext);
      }
    };
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
    state: { contextArgs: unknown[]; payload: unknown },
  ): Promise<unknown> {
    const guards = this.runtime.guardsContextCreator.create(
      instance,
      callback,
      handler.moduleKey,
      contextId,
      handler.inquirerId,
    );
    await this.activateGuards(guards, state.contextArgs, instance, callback);

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

    const invoke = this.createInvoker(
      handler,
      instance,
      callback,
      pipes,
      state.contextArgs,
    );

    const result =
      interceptors.length === 0
        ? await invoke()
        : await this.runtime.interceptorsConsumer.intercept(
            interceptors,
            state.contextArgs,
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
    contextArgs: unknown[],
  ): () => Promise<unknown> {
    return async () => {
      const args = [...contextArgs];
      if (pipes.length > 0) {
        args[0] = await this.applyPayloadPipes(
          args[0],
          pipes,
          handler.paramTypes,
        );
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
