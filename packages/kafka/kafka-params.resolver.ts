import { PipeTransform } from '@nestjs/common';
import { Controller } from '@nestjs/common/interfaces';
import { ContextUtils } from '@nestjs/core/helpers/context-utils';
import { ExecutionContextHost } from '@nestjs/core/helpers/execution-context-host';

/**
 * The reflected metadata one custom parameter decorator stores on a handler
 * method, mirroring the shape `assignCustomParameterMetadata` writes under
 * `ROUTE_ARGS_METADATA`. Captured locally so the resolver does not depend on the
 * util's internal type.
 */
interface KafkaParamMetadata {
  index: number;
  factory: (data: unknown, context: ExecutionContextHost) => unknown;
  data: unknown;
  pipes: unknown[];
}

/**
 * One resolved parameter: where it goes in the args array, how to extract its
 * value from the execution context, its declared metatype, and the raw pipe
 * metadata to materialise lazily.
 */
interface ResolvedParam {
  index: number;
  metatype: unknown;
  extract: (context: ExecutionContextHost) => unknown;
  pipeMetadata: unknown[];
}

/**
 * The slice of the pipe machinery the resolver needs, matching the runtime the
 * {@link KafkaContextCreator} already builds.
 *
 * `createConcrete` is called lazily — on the first consumed message rather than
 * when the handler is wired — so the owning module context is already set and
 * class-based param pipes resolve to their DI instances.
 */
export interface KafkaParamsPipes {
  createConcrete: (pipes: unknown[]) => PipeTransform[];
  apply: (
    value: unknown,
    index: number,
    metatype: unknown,
    pipes: PipeTransform[],
  ) => Promise<unknown>;
}

const ROUTE_ARGS_METADATA = '__routeArguments__';

/**
 * Resolves `@KafkaMessage()`, `@KafkaHeaders()`, and `@KafkaCtx()` parameters
 * for a single handler method.
 *
 * It reads the custom-parameter metadata Nest's {@link createParamDecorator}
 * stores, then for every consumed message builds the handler's argument array by
 * running each decorator's factory against the message's execution context and
 * applying the param-level pipes — the same contract `@nestjs/microservices`
 * honours for `@Payload()` / `@Ctx()`.
 *
 * When a handler declares no parameter decorators the resolver reports
 * {@link hasParams} as `false`, so the caller can keep the positional
 * `[payload, context]` calling convention used by handlers that take the payload
 * as their first argument.
 *
 * @internal
 */
export class KafkaParamsResolver {
  private readonly contextUtils = new ContextUtils();
  private readonly params: ResolvedParam[];
  private readonly argsLength: number;
  private concretePipes?: PipeTransform[][];

  constructor(
    metatype: Controller['constructor'],
    methodName: string,
    paramTypes: unknown[],
    private readonly pipesRuntime: KafkaParamsPipes,
  ) {
    const metadata = this.reflectMetadata(metatype, methodName);
    const entries = Object.values(metadata);
    this.params = entries.map(entry => this.resolveParam(entry, paramTypes));
    this.argsLength = entries.length
      ? Math.max(...entries.map(entry => entry.index)) + 1
      : 0;
  }

  /**
   * Whether the handler declares at least one parameter decorator.
   */
  hasParams(): boolean {
    return this.params.length > 0;
  }

  /**
   * Build the handler's argument array for one message, running every parameter
   * factory and its pipes against the supplied execution context.
   */
  async resolve(context: ExecutionContextHost): Promise<unknown[]> {
    const pipes = this.materialisePipes();
    const args = new Array<unknown>(this.argsLength).fill(undefined);
    await Promise.all(
      this.params.map(async (param, position) => {
        const value = param.extract(context);
        args[param.index] = await this.applyPipes(value, param, pipes[position]);
      }),
    );
    return args;
  }

  /**
   * Materialise the param pipes once, on the first consumed message, by which
   * time the owning module context is set so class pipes resolve to instances.
   */
  private materialisePipes(): PipeTransform[][] {
    this.concretePipes ??= this.params.map(param =>
      this.pipesRuntime.createConcrete(param.pipeMetadata),
    );
    return this.concretePipes;
  }

  private reflectMetadata(
    metatype: Controller['constructor'],
    methodName: string,
  ): Record<string, KafkaParamMetadata> {
    return (
      (Reflect.getMetadata(ROUTE_ARGS_METADATA, metatype, methodName) as
        | Record<string, KafkaParamMetadata>
        | undefined) ?? {}
    );
  }

  private resolveParam(
    entry: KafkaParamMetadata,
    paramTypes: unknown[],
  ): ResolvedParam {
    const extract = this.contextUtils.getCustomFactory(
      entry.factory as never,
      entry.data,
      args => args[0] as ExecutionContextHost,
    );
    return {
      index: entry.index,
      metatype: paramTypes[entry.index],
      extract: (context: ExecutionContextHost) => extract(context),
      pipeMetadata: entry.pipes,
    };
  }

  private applyPipes(
    value: unknown,
    param: ResolvedParam,
    pipes: PipeTransform[],
  ): Promise<unknown> {
    if (pipes.length === 0) {
      return Promise.resolve(value);
    }
    return this.pipesRuntime.apply(value, param.index, param.metatype, pipes);
  }
}
