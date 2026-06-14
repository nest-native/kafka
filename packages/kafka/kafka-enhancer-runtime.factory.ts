import { ApplicationConfig } from '@nestjs/core';
import { ExternalExceptionFilterContext } from '@nestjs/core/exceptions/external-exception-filter-context';
import { GuardsConsumer } from '@nestjs/core/guards/guards-consumer';
import { GuardsContextCreator } from '@nestjs/core/guards/guards-context-creator';
import { InterceptorsConsumer } from '@nestjs/core/interceptors/interceptors-consumer';
import { InterceptorsContextCreator } from '@nestjs/core/interceptors/interceptors-context-creator';
import { ModulesContainer } from '@nestjs/core/injector/modules-container';
import { PipesConsumer } from '@nestjs/core/pipes/pipes-consumer';
import { PipesContextCreator } from '@nestjs/core/pipes/pipes-context-creator';
import { KafkaEnhancerRuntime } from './kafka-context-creator';

interface ContainerRefLike {
  getModules: () => ModulesContainer;
}

/**
 * Build the Nest enhancer runtime the {@link KafkaContextCreator} uses to run
 * guards, interceptors, pipes, and exception filters around handler methods.
 *
 * This isolates the Nest internal enhancer wiring to a single boundary so a
 * future Nest major upgrade is cheap to validate, mirroring the approach the
 * sibling nest-native packages take for their custom transports.
 */
export function createKafkaEnhancerRuntime(
  modulesContainer: ModulesContainer,
  applicationConfig: ApplicationConfig,
): KafkaEnhancerRuntime {
  const containerRef: ContainerRefLike = {
    getModules: () => modulesContainer,
  };

  return {
    guardsContextCreator: new GuardsContextCreator(
      containerRef as never,
      applicationConfig,
    ),
    guardsConsumer: new GuardsConsumer(),
    interceptorsContextCreator: new InterceptorsContextCreator(
      containerRef as never,
      applicationConfig,
    ),
    interceptorsConsumer: new InterceptorsConsumer(),
    pipesContextCreator: new PipesContextCreator(
      containerRef as never,
      applicationConfig,
    ),
    pipesConsumer: new PipesConsumer(),
    exceptionFiltersContext: new ExternalExceptionFilterContext(
      containerRef as never,
      applicationConfig,
    ),
  };
}
