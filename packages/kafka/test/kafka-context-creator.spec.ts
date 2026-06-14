import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { Controller } from '@nestjs/common/interfaces';
import { ExecutionContextHost } from '@nestjs/core/helpers/execution-context-host';
import { STATIC_CONTEXT } from '@nestjs/core/injector/constants';
import { of } from 'rxjs';
import {
  KafkaContextCreator,
  KafkaEnhancerRuntime,
  KafkaHandlerContext,
} from '../kafka-context-creator';
import { KafkaContext } from '../kafka-context';

/**
 * A no-op enhancer runtime: no guards, interceptors, or pipes, and an exception
 * handler that rethrows. Individual tests override pieces as needed.
 */
function createRuntime(
  overrides: Partial<KafkaEnhancerRuntime> = {},
): KafkaEnhancerRuntime {
  return {
    guardsContextCreator: { create: () => [] },
    guardsConsumer: { tryActivate: async () => true },
    interceptorsContextCreator: { create: () => [] },
    interceptorsConsumer: { intercept: async () => undefined },
    pipesContextCreator: { create: () => [] },
    pipesConsumer: { apply: async value => value },
    exceptionFiltersContext: {
      create: () => ({
        next: (error: Error) => {
          throw error;
        },
      }),
    },
    ...overrides,
  };
}

function handlerContext(
  overrides: Partial<KafkaHandlerContext> & {
    callback: KafkaHandlerContext['callback'];
    resolveInstance: KafkaHandlerContext['resolveInstance'];
  },
): KafkaHandlerContext {
  return {
    methodName: 'handle',
    moduleKey: '',
    paramTypes: [],
    resolveContextId: () => STATIC_CONTEXT,
    ...overrides,
  };
}

function invocation(payload: unknown) {
  return {
    payload,
    context: new KafkaContext('topic', 0, { value: null }),
  };
}

describe('KafkaContextCreator', () => {
  it('invokes the resolved instance method with the payload', async () => {
    const calls: unknown[] = [];
    const instance = {
      handle(payload: unknown) {
        calls.push(payload);
        return 'ok';
      },
    } as unknown as Controller;

    const creator = new KafkaContextCreator(createRuntime());
    const run = creator.create(
      handlerContext({
        callback: (instance as Record<string, unknown>).handle as never,
        resolveInstance: async () => instance,
      }),
    );

    const result = await run(invocation({ id: 1 }));

    assert.equal(result, 'ok');
    assert.deepEqual(calls, [{ id: 1 }]);
  });

  it('falls back to the bound callback when the instance lacks the method', async () => {
    const seen: unknown[] = [];
    const callback = function fallback(payload: unknown) {
      seen.push(payload);
      return 'fallback';
    };
    // The resolved instance has no `handle` method, forcing the callback path.
    const instance = {} as unknown as Controller;

    const creator = new KafkaContextCreator(createRuntime());
    const run = creator.create(
      handlerContext({
        callback: callback as never,
        resolveInstance: async () => instance,
      }),
    );

    const result = await run(invocation('payload'));

    assert.equal(result, 'fallback');
    assert.deepEqual(seen, ['payload']);
  });

  it('unwraps an Observable returned directly by the handler', async () => {
    const instance = {
      handle() {
        return of('observable-result');
      },
    } as unknown as Controller;

    const creator = new KafkaContextCreator(createRuntime());
    const run = creator.create(
      handlerContext({
        callback: (instance as Record<string, unknown>).handle as never,
        resolveInstance: async () => instance,
      }),
    );

    assert.equal(await run(invocation('x')), 'observable-result');
  });

  it('unwraps an Observable returned by an interceptor', async () => {
    const instance = {
      handle() {
        return 'inner';
      },
    } as unknown as Controller;

    const runtime = createRuntime({
      interceptorsContextCreator: { create: () => [{}] },
      interceptorsConsumer: {
        intercept: async (_i, _a, _inst, _cb, next) => {
          await next();
          return of('from-interceptor');
        },
      },
    });

    const creator = new KafkaContextCreator(runtime);
    const run = creator.create(
      handlerContext({
        callback: (instance as Record<string, unknown>).handle as never,
        resolveInstance: async () => instance,
      }),
    );

    assert.equal(await run(invocation('x')), 'from-interceptor');
  });

  it('routes a thrown error through the exception handler', async () => {
    const failure = new Error('boom');
    const instance = {
      handle() {
        throw failure;
      },
    } as unknown as Controller;

    let received: unknown;
    const runtime = createRuntime({
      exceptionFiltersContext: {
        create: () => ({
          next: (error: Error, host: ExecutionContextHost) => {
            received = { error, type: host.getType() };
            return 'handled';
          },
        }),
      },
    });

    const creator = new KafkaContextCreator(runtime);
    const run = creator.create(
      handlerContext({
        callback: (instance as Record<string, unknown>).handle as never,
        resolveInstance: async () => instance,
      }),
    );

    const result = await run(invocation('x'));

    assert.equal(result, 'handled');
    assert.deepEqual(received, { error: failure, type: 'rpc' });
  });

  it('applies pipes to the payload before invoking the handler', async () => {
    const received: unknown[] = [];
    const instance = {
      handle(payload: unknown) {
        received.push(payload);
      },
    } as unknown as Controller;

    const runtime = createRuntime({
      pipesContextCreator: { create: () => [{ transform: v => v }] },
      pipesConsumer: {
        apply: async value => ({ wrapped: value }),
      },
    });

    const creator = new KafkaContextCreator(runtime);
    const run = creator.create(
      handlerContext({
        callback: (instance as Record<string, unknown>).handle as never,
        paramTypes: [Object],
        resolveInstance: async () => instance,
      }),
    );

    await run(invocation('raw'));

    assert.deepEqual(received, [{ wrapped: 'raw' }]);
  });
});
