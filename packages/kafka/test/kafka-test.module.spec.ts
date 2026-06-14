import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import {
  BadRequestException,
  CanActivate,
  ExecutionContext,
  Injectable,
  Module,
  UseGuards,
} from '@nestjs/common';
import { Test } from '@nestjs/testing';
import { KafkaConsumer } from '../kafka-consumer.decorator';
import { KafkaHandler } from '../kafka-handler.decorator';
import { KafkaCtx, KafkaMessage } from '../kafka-params.decorators';
import { KafkaContext } from '../kafka-context';
import { KafkaProducerService } from '../kafka-producer.service';
import { KafkaModuleOptions } from '../interfaces';
import { KAFKA_MODULE_OPTIONS } from '../tokens';
import { KAFKA_TEST_BROKER as KAFKA_TEST_BROKER_TOKEN } from '../tokens';
import { InMemoryKafkaBroker } from '../testing/in-memory-kafka-broker';
import {
  InjectKafkaTestBroker,
  KAFKA_TEST_BROKER,
  KafkaTestModule,
  KafkaTestModuleOptions,
} from '../testing/kafka-test.module';

interface OrderPlaced {
  id: string;
}

@Injectable()
class OrderLog {
  readonly seen: { order: OrderPlaced; topic: string }[] = [];
}

@Injectable()
@KafkaConsumer('orders.placed', { groupId: 'orders-test' })
class OrdersConsumer {
  constructor(private readonly log: OrderLog) {}

  @KafkaHandler()
  handle(@KafkaMessage() order: OrderPlaced, @KafkaCtx() context: KafkaContext): void {
    this.log.seen.push({ order, topic: context.getTopic() });
  }
}

@Injectable()
class OrdersService {
  constructor(private readonly producer: KafkaProducerService) {}

  async place(order: OrderPlaced): Promise<void> {
    await this.producer.send({
      topic: 'orders.placed',
      messages: [{ key: order.id, value: JSON.stringify(order) }],
    });
  }
}

@Module({
  providers: [OrderLog, OrdersConsumer, OrdersService],
})
class OrdersModule {}

@Injectable()
class TenantGuard implements CanActivate {
  static deny = false;

  canActivate(_context: ExecutionContext): boolean {
    if (TenantGuard.deny) {
      throw new BadRequestException('no tenant');
    }
    return true;
  }
}

@Injectable()
@KafkaConsumer('secured', { groupId: 'secured-test' })
@UseGuards(TenantGuard)
class SecuredConsumer {
  readonly handled: string[] = [];

  @KafkaHandler()
  handle(@KafkaMessage() value: string): void {
    this.handled.push(value);
  }
}

@Module({ providers: [TenantGuard, SecuredConsumer] })
class SecuredModule {}

@Injectable()
class ConfigService {
  readonly clientId = 'async-test';
}

async function bootstrap(
  imports: Parameters<typeof Test.createTestingModule>[0]['imports'],
): Promise<{
  close: () => Promise<void>;
  get: <T>(token: unknown) => T;
}> {
  // A transport-only module needs no HTTP adapter, so init the testing module
  // directly: `.init()` fires `onApplicationBootstrap` (where the consumer
  // explorer subscribes) and `.close()` fires the shutdown hooks.
  const moduleRef = await Test.createTestingModule({ imports }).compile();
  await moduleRef.init();
  return {
    close: () => moduleRef.close(),
    get: <T,>(token: unknown) =>
      moduleRef.get<T>(token as never, { strict: false }),
  };
}

describe('KafkaTestModule', () => {
  it('runs the producer and consumer through the in-memory broker', async () => {
    const app = await bootstrap([KafkaTestModule.forRoot(), OrdersModule]);

    const broker = app.get<InMemoryKafkaBroker>(KAFKA_TEST_BROKER);
    const log = app.get<OrderLog>(OrderLog);

    await app.get<OrdersService>(OrdersService).place({ id: 'order-1' });

    assert.equal(log.seen.length, 1);
    assert.deepEqual(log.seen[0].order, { id: 'order-1' });
    assert.equal(log.seen[0].topic, 'orders.placed');
    assert.deepEqual(broker.getSentTo('orders.placed'), [
      { key: 'order-1', value: JSON.stringify({ id: 'order-1' }) },
    ]);

    await app.close();
  });

  it('emits a message straight to a consumer without a producing service', async () => {
    const app = await bootstrap([KafkaTestModule.forRoot(), OrdersModule]);

    const broker = app.get<InMemoryKafkaBroker>(KAFKA_TEST_BROKER);
    const log = app.get<OrderLog>(OrderLog);

    await broker.emit('orders.placed', { value: JSON.stringify({ id: 'order-2' }) });

    assert.deepEqual(log.seen[0].order, { id: 'order-2' });

    await app.close();
  });

  it('runs the full enhancer pipeline so a denying guard maps to commit', async () => {
    TenantGuard.deny = true;
    const app = await bootstrap([KafkaTestModule.forRoot(), SecuredModule]);
    const broker = app.get<InMemoryKafkaBroker>(KAFKA_TEST_BROKER);
    const consumer = app.get<SecuredConsumer>(SecuredConsumer);

    // The guard throws BadRequestException -> default mapper commits, so the
    // handler never runs and the broker does not blow up.
    await broker.emit('secured', { value: 'blocked' });
    assert.deepEqual(consumer.handled, []);

    TenantGuard.deny = false;
    await broker.emit('secured', { value: 'allowed' });
    assert.deepEqual(consumer.handled, ['allowed']);

    await app.close();
  });

  it('forwards module options (errorMapper) to the underlying KafkaModule', async () => {
    const errorMapper = (): 'commit' => 'commit';
    const options: KafkaTestModuleOptions = { clientId: 'orders', errorMapper };

    const moduleRef = await Test.createTestingModule({
      imports: [KafkaTestModule.forRoot(options)],
    }).compile();

    const resolved = moduleRef.get<KafkaModuleOptions>(KAFKA_MODULE_OPTIONS);
    assert.equal(resolved.clientId, 'orders');
    assert.equal(resolved.errorMapper, errorMapper);
    assert.equal(typeof resolved.driverFactory, 'function');
  });

  it('reuses a provided broker instance', async () => {
    const broker = new InMemoryKafkaBroker();
    const app = await bootstrap([
      KafkaTestModule.forRoot({ broker }),
      OrdersModule,
    ]);

    assert.equal(app.get<InMemoryKafkaBroker>(KAFKA_TEST_BROKER), broker);
    await app.get<OrdersService>(OrdersService).place({ id: 'order-3' });
    assert.equal(broker.getSentTo('orders.placed').length, 1);

    await app.close();
  });

  it('is global by default and honours an explicit opt-out', () => {
    assert.equal(KafkaTestModule.forRoot().global, true);
    assert.equal(KafkaTestModule.forRoot({ isGlobal: true }).global, true);
    assert.equal(KafkaTestModule.forRoot({ isGlobal: false }).global, false);
  });

  it('re-exports the KAFKA_TEST_BROKER token from the testing entrypoint', () => {
    assert.equal(KAFKA_TEST_BROKER, KAFKA_TEST_BROKER_TOKEN);
  });

  it('exposes the broker through the InjectKafkaTestBroker decorator', async () => {
    @Injectable()
    class BrokerInspector {
      constructor(
        @InjectKafkaTestBroker() readonly broker: InMemoryKafkaBroker,
      ) {}
    }

    @Module({ providers: [BrokerInspector] })
    class InspectorModule {}

    const app = await bootstrap([
      KafkaTestModule.forRoot(),
      InspectorModule,
    ]);

    const inspector = app.get<BrokerInspector>(BrokerInspector);
    assert.ok(inspector.broker instanceof InMemoryKafkaBroker);
    assert.equal(inspector.broker, app.get(KAFKA_TEST_BROKER));

    await app.close();
  });

  it('resolves options asynchronously through forRootAsync', async () => {
    const moduleRef = await Test.createTestingModule({
      imports: [
        KafkaTestModule.forRootAsync({
          inject: [ConfigService],
          extraProviders: [ConfigService],
          useFactory: (config: ConfigService) => ({ clientId: config.clientId }),
        }),
      ],
    }).compile();

    const resolved = moduleRef.get<KafkaModuleOptions>(KAFKA_MODULE_OPTIONS);
    assert.equal(resolved.clientId, 'async-test');
    assert.equal(typeof resolved.driverFactory, 'function');
  });

  it('runs the transport end-to-end when wired through forRootAsync', async () => {
    const broker = new InMemoryKafkaBroker();
    const app = await bootstrap([
      KafkaTestModule.forRootAsync({
        broker,
        useFactory: () => ({ clientId: 'async-e2e' }),
      }),
      OrdersModule,
    ]);

    assert.equal(app.get<InMemoryKafkaBroker>(KAFKA_TEST_BROKER), broker);
    await app.get<OrdersService>(OrdersService).place({ id: 'order-4' });
    assert.equal(app.get<OrderLog>(OrderLog).seen.length, 1);

    await app.close();
  });

  it('defaults forRootAsync isGlobal to true', () => {
    const dynamicModule = KafkaTestModule.forRootAsync({
      useFactory: () => ({}),
    });
    assert.equal(dynamicModule.global, true);
  });

  it('lets forRootAsync opt out of global registration', () => {
    const dynamicModule = KafkaTestModule.forRootAsync({
      isGlobal: false,
      useFactory: () => ({}),
    });
    assert.equal(dynamicModule.global, false);
  });
});
