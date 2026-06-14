import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { Injectable } from '@nestjs/common';
import { Test } from '@nestjs/testing';
import { KafkaModule } from '../kafka.module';
import {
  KafkaClientDriver,
  KafkaDriverFactory,
  KafkaDriverProducer,
} from '../driver';
import { InjectKafkaProducer } from '../inject-kafka-producer.decorator';

function createFakeProducer(): KafkaDriverProducer {
  return {
    connect: async () => {},
    disconnect: async () => {},
    send: async () => [],
    sendBatch: async () => [],
    transaction: async () => ({
      send: async () => [],
      sendBatch: async () => [],
      sendOffsets: async () => {},
      commit: async () => {},
      abort: async () => {},
    }),
  };
}

describe('InjectKafkaProducer', () => {
  it('injects the raw producer into a provider', async () => {
    const producer = createFakeProducer();
    const factory: KafkaDriverFactory = () => {
      const driver: KafkaClientDriver = {
        createProducer: () => producer,
        createConsumer: () => ({
          connect: async () => {},
          disconnect: async () => {},
          subscribe: async () => {},
          run: async () => {},
        }),
      };
      return driver;
    };

    @Injectable()
    class OutboxService {
      constructor(
        @InjectKafkaProducer() readonly producer: KafkaDriverProducer,
      ) {}
    }

    const module = await Test.createTestingModule({
      imports: [KafkaModule.forRoot({ driverFactory: factory })],
      providers: [OutboxService],
    }).compile();

    assert.equal(module.get(OutboxService).producer, producer);
  });
});
