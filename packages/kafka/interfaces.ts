import { ModuleMetadata, Provider } from '@nestjs/common';

/**
 * Configuration for {@link KafkaModule.forRoot}.
 *
 * At this scaffold milestone the module only wires global configuration; the
 * consumer decorators (`@KafkaConsumer`, `@KafkaHandler`), the parameter
 * decorators, and the producer service land in later milestones and will read
 * from these options.
 */
export interface KafkaModuleOptions {
  /**
   * Whether to register this module globally so the configuration is available
   * to every feature module without re-importing.
   *
   * @default true
   */
  isGlobal?: boolean;

  /**
   * Default client identifier reported to the broker for connections opened by
   * the consumers and producer added in later milestones.
   */
  clientId?: string;
}

/**
 * Configuration for {@link KafkaModule.forRootAsync}.
 */
export interface KafkaModuleAsyncOptions
  extends Pick<ModuleMetadata, 'imports'> {
  /**
   * Whether to register this module globally.
   *
   * @default true
   */
  isGlobal?: boolean;

  /**
   * Providers to inject into {@link KafkaModuleAsyncOptions.useFactory}.
   */
  inject?: any[];

  /**
   * Additional providers registered alongside the resolved options.
   */
  extraProviders?: Provider[];

  /**
   * Factory that resolves the {@link KafkaModuleOptions} asynchronously.
   */
  useFactory: (
    ...args: any[]
  ) => KafkaModuleOptions | Promise<KafkaModuleOptions>;
}
