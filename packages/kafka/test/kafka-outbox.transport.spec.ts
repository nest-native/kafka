import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { KafkaRequestSerializer } from '@nestjs/microservices/serializers/kafka-request.serializer';
import {
  ClientProxyTransport,
  NonRetryableMessageError,
  OutboxPublishTimeoutError,
  OutboxTransport,
  type OutboxEnvelope,
  type OutboxMessage,
} from '@nestjs/outbox';
import { of } from 'rxjs';
import type { KafkaDriverProducer, KafkaSendRecord } from '../driver';
import { KafkaProducerService } from '../kafka-producer.service';
import { toPermanentFailure } from '../outbox/kafka-outbox.errors';
import {
  KafkaOutboxTransport,
  type KafkaOutboxTransportOptions,
} from '../outbox';
import { createMockKafkaProducer } from '../testing/mock-kafka-producer';

/** An outbox message as the relay hands it to a transport. */
function outboxMessage(overrides: Partial<OutboxMessage> = {}): OutboxMessage {
  return {
    id: '0192f5c4-7d1e-7c3a-9b6e-3f1d2a4b5c6d',
    topic: 'orders.placed',
    payload: {
      orderId: 'order-1',
      customer: 'Zoë — “Ünïcode” ☕',
      lines: [{ sku: 'A-1', quantity: 2, price: 9.5 }],
      gift: false,
      note: null,
    },
    headers: { 'x-tenant': 'acme', 'x-trace': 'abc123' },
    key: 'order-1',
    createdAt: 1_763_700_000_000,
    availableAt: 1_763_700_000_000,
    attempts: 0,
    lastError: null,
    ...overrides,
  };
}

const live = (): { signal: AbortSignal } => ({
  signal: new AbortController().signal,
});

/** A transport over a recording producer, plus the records it produced. */
function recording(options?: KafkaOutboxTransportOptions): {
  transport: KafkaOutboxTransport;
  sent: KafkaSendRecord[];
} {
  const { producer, calls } = createMockKafkaProducer();
  return {
    transport: new KafkaOutboxTransport(new KafkaProducerService(producer), options),
    sent: calls.send,
  };
}

/** A transport whose producer rejects every send with `error`. */
function failingWith(error: unknown): {
  transport: KafkaOutboxTransport;
  attempts: KafkaSendRecord[];
} {
  const { producer } = createMockKafkaProducer();
  const attempts: KafkaSendRecord[] = [];
  const failing: KafkaDriverProducer = {
    ...producer,
    send: async record => {
      attempts.push(record);
      throw error;
    },
  };
  return {
    transport: new KafkaOutboxTransport(new KafkaProducerService(failing)),
    attempts,
  };
}

/**
 * The error `produce()` throws before a record is sent: node-rdkafka's
 * `LibrdKafkaError`, an `Error` carrying librdkafka's code, not a KafkaJS error.
 */
function librdkafkaError(code: number, message: string): Error {
  return Object.assign(new Error(message), {
    code,
    errno: code,
    origin: code > 0 ? 'kafka' : 'local',
  });
}

/** The error a failed delivery report becomes in Confluent's KafkaJS layer. */
class KafkaJSProtocolErrorLike extends Error {
  readonly name = 'KafkaJSProtocolError';
  readonly retriable = false;
  readonly fatal = false;
  readonly abortable = false;

  constructor(
    message: string,
    readonly code: number,
    readonly type: string,
  ) {
    super(message);
  }
}

describe('KafkaOutboxTransport', () => {
  describe('the default record', () => {
    it("produces the message's topic, key and headers, with the envelope's JSON as the value", async () => {
      const message = outboxMessage();
      const { transport, sent } = recording();

      await transport.publish(message, live());

      assert.deepEqual(sent, [
        {
          topic: 'orders.placed',
          messages: [
            {
              key: 'order-1',
              value:
                '{"id":"0192f5c4-7d1e-7c3a-9b6e-3f1d2a4b5c6d","topic":"orders.placed",' +
                '"key":"order-1","headers":{"x-tenant":"acme","x-trace":"abc123"},' +
                '"createdAt":1763700000000,"payload":{"orderId":"order-1",' +
                '"customer":"Zoë — “Ünïcode” ☕","lines":[{"sku":"A-1","quantity":2,' +
                '"price":9.5}],"gift":false,"note":null}}',
              headers: { 'x-tenant': 'acme', 'x-trace': 'abc123' },
            },
          ],
        },
      ]);
    });

    it('produces a null key for a message without one, and lets the partitioner pick the partition', async () => {
      const { transport, sent } = recording();

      await transport.publish(outboxMessage({ key: null, headers: {} }), live());

      const [produced] = sent[0].messages;
      assert.equal(produced.key, null);
      assert.deepEqual(produced.headers, {});
      assert.equal('partition' in produced, false);
      assert.equal(JSON.parse(produced.value as string).key, null);
    });

    it("hands the client a copy of the message's headers, never the stored object", async () => {
      const message = outboxMessage();
      const { transport, sent } = recording();

      await transport.publish(message, live());

      const { headers } = sent[0].messages[0];
      assert.deepEqual(headers, message.headers);
      assert.notEqual(headers, message.headers);
    });

    for (const variant of [
      { name: 'a keyed message with headers', message: outboxMessage() },
      {
        name: 'a message without a key or headers',
        message: outboxMessage({ key: null, headers: {}, payload: ['a', 1] }),
      },
    ]) {
      it(`matches, byte for byte, what the official ClientProxyTransport sends through a ClientKafka: ${variant.name}`, async () => {
        // The official path: ClientProxyTransport recognizes a ClientKafka by its
        // subscribeToResponseOf() and emits toKafkaRecord's packet, which
        // ClientKafka serializes with KafkaRequestSerializer and produces to the
        // packet's pattern. The real-broker suite compares the consumed records.
        const emitted: { pattern: unknown; data: unknown }[] = [];
        const clientKafka = {
          subscribeToResponseOf: () => undefined,
          emit: (pattern: unknown, data: unknown) => {
            emitted.push({ pattern, data });
            return of(undefined);
          },
        };
        const Official = ClientProxyTransport('OFFICIAL_KAFKA');
        await new Official(clientKafka).publish(variant.message, live());
        const serialized = await new KafkaRequestSerializer().serialize(
          emitted[0].data,
        );

        const { transport, sent } = recording();
        await transport.publish(variant.message, live());

        assert.deepEqual(sent, [
          { topic: emitted[0].pattern, messages: [serialized] },
        ]);
      });
    }
  });

  describe('toRecord', () => {
    it('replaces only the fields it returns', async () => {
      const message = outboxMessage();
      const { transport, sent } = recording({
        toRecord: ({ topic }) => ({ topic: `staging.${topic}` }),
      });
      const reference = recording();

      await transport.publish(message, live());
      await reference.transport.publish(message, live());

      assert.equal(sent[0].topic, 'staging.orders.placed');
      assert.deepEqual(sent[0].messages, reference.sent[0].messages);
    });

    it('replaces every field it returns, the partition included', async () => {
      const value = Buffer.from('raw bytes');
      const { transport, sent } = recording({
        toRecord: () => ({
          topic: 'audit',
          key: 'tenant-7',
          value,
          headers: { 'content-type': 'application/octet-stream' },
          partition: 3,
        }),
      });

      await transport.publish(outboxMessage(), live());

      assert.deepEqual(sent, [
        {
          topic: 'audit',
          messages: [
            {
              key: 'tenant-7',
              value,
              headers: { 'content-type': 'application/octet-stream' },
              partition: 3,
            },
          ],
        },
      ]);
    });

    it('keeps a null key or value it returns instead of the default', async () => {
      const { transport, sent } = recording({
        toRecord: () => ({ key: null, value: null }),
      });

      await transport.publish(outboxMessage(), live());

      const [produced] = sent[0].messages;
      assert.equal(produced.key, null);
      assert.equal(produced.value, null);
    });

    it('keeps the default record when it returns nothing', async () => {
      const message = outboxMessage();
      const { transport, sent } = recording({ toRecord: () => undefined });
      const reference = recording();

      await transport.publish(message, live());
      await reference.transport.publish(message, live());

      assert.deepEqual(sent, reference.sent);
    });

    it('receives the message and its envelope, and is called without the transport as `this`', async () => {
      const message = outboxMessage();
      const seen: { self: unknown; message: OutboxMessage; envelope: OutboxEnvelope }[] = [];
      const { transport } = recording({
        toRecord: function (this: unknown, received, envelope) {
          seen.push({ self: this, message: received, envelope });
          return {};
        },
      });

      await transport.publish(message, live());

      assert.equal(seen.length, 1);
      assert.equal(seen[0].self, undefined);
      assert.equal(seen[0].message, message);
      assert.deepEqual(seen[0].envelope, {
        id: message.id,
        topic: message.topic,
        key: message.key,
        headers: message.headers,
        createdAt: message.createdAt,
        payload: message.payload,
      });
    });

    it("expresses a custom wire: reference-app's prefixed topic, bare payload, and id headers", async () => {
      // reference-app's wire contract: the key and x-idempotency-key are the
      // business key (the id when there is none), x-event-id is the message
      // id, the value is the bare JSON payload, and topics carry a prefix.
      const prefix = 'ci-42.';
      const { transport, sent } = recording({
        toRecord: message => {
          const idempotencyKey = message.headers['x-idempotency-key'] ?? message.id;
          return {
            topic: `${prefix}${message.topic}`,
            key: idempotencyKey,
            value: JSON.stringify(message.payload),
            headers: {
              'x-event-id': message.id,
              'x-idempotency-key': idempotencyKey,
            },
          };
        },
      });
      const message = outboxMessage({
        topic: 'user.invited',
        key: null,
        payload: { orgId: 'o1', userId: 'u1' },
        headers: { 'x-idempotency-key': 'user.invited:o1:u1' },
      });

      await transport.publish(message, live());

      assert.deepEqual(sent, [
        {
          topic: 'ci-42.user.invited',
          messages: [
            {
              key: 'user.invited:o1:u1',
              value: '{"orgId":"o1","userId":"u1"}',
              headers: {
                'x-event-id': message.id,
                'x-idempotency-key': 'user.invited:o1:u1',
              },
            },
          ],
        },
      ]);
    });

    it('fails the attempt with what it throws, and produces nothing', async () => {
      const boom = new Error('mapper bug');
      const rejected = new NonRetryableMessageError('cannot map this one');
      let thrown: Error = boom;
      const { transport, sent } = recording({
        toRecord: () => {
          throw thrown;
        },
      });

      await assert.rejects(transport.publish(outboxMessage(), live()), error => error === boom);
      thrown = rejected;
      await assert.rejects(
        transport.publish(outboxMessage(), live()),
        error => error === rejected,
      );
      assert.equal(sent.length, 0);
    });
  });

  describe('the relay signal', () => {
    it('does not produce once the signal has aborted, and rejects with its reason', async () => {
      const { transport, sent } = recording();
      const controller = new AbortController();
      const timeout = new OutboxPublishTimeoutError(10_000);
      controller.abort(timeout);

      await assert.rejects(
        transport.publish(outboxMessage(), { signal: controller.signal }),
        error => error === timeout,
      );
      assert.equal(sent.length, 0);
    });
  });

  describe('produce failures', () => {
    const permanent = [
      {
        code: 10,
        failure: 'MSG_SIZE_TOO_LARGE',
        // Over the producer's message.max.bytes: produce() throws before sending.
        shape: 'the LibrdKafkaError produce() throws',
        error: librdkafkaError(10, 'Broker: Message size too large'),
      },
      {
        code: 10,
        failure: 'MSG_SIZE_TOO_LARGE',
        // Over the topic's max.message.bytes: the broker refuses the batch.
        shape: 'a delivery report',
        error: new KafkaJSProtocolErrorLike(
          'Broker: Message size too large',
          10,
          'MSG_SIZE_TOO_LARGE',
        ),
      },
      {
        code: 17,
        failure: 'TOPIC_EXCEPTION',
        shape: 'a delivery report',
        error: new KafkaJSProtocolErrorLike('Broker: Invalid topic', 17, 'TOPIC_EXCEPTION'),
      },
      {
        code: 29,
        failure: 'TOPIC_AUTHORIZATION_FAILED',
        shape: 'a delivery report',
        error: new KafkaJSProtocolErrorLike(
          'Broker: Topic authorization failed',
          29,
          'TOPIC_AUTHORIZATION_FAILED',
        ),
      },
      {
        code: 87,
        failure: 'INVALID_RECORD',
        shape: 'a delivery report',
        error: new KafkaJSProtocolErrorLike(
          'Broker: Broker failed to validate record',
          87,
          'INVALID_RECORD',
        ),
      },
    ];

    for (const { code, failure, shape, error } of permanent) {
      it(`dead-letters ${failure} (code ${code}, from ${shape}) as a NonRetryableMessageError with the client's error as its cause`, async () => {
        const { transport, attempts } = failingWith(error);

        await assert.rejects(transport.publish(outboxMessage(), live()), thrown => {
          assert.equal(thrown instanceof NonRetryableMessageError, true);
          const nonRetryable = thrown as NonRetryableMessageError;
          assert.equal((nonRetryable as { cause?: unknown }).cause, error);
          assert.equal(
            nonRetryable.message,
            `Kafka refused the record for topic "orders.placed" permanently ` +
              `(${failure}, code ${code}): ${error.message}`,
          );
          return true;
        });
        assert.equal(attempts.length, 1);
      });
    }

    const retryable = [
      // Another record of the same batch was invalid.
      librdkafkaError(-138, 'Local: A different record in the batch was invalid'),
      // The batch, not the record, was over the segment size.
      new KafkaJSProtocolErrorLike(
        'Broker: Message batch larger than configured server segment size',
        18,
        'RECORD_LIST_TOO_LARGE',
      ),
      new KafkaJSProtocolErrorLike('Local: Message timed out', -192, 'MSG_TIMED_OUT'),
      new KafkaJSProtocolErrorLike(
        'Broker: Unknown topic or partition',
        3,
        'UNKNOWN_TOPIC_OR_PART',
      ),
      librdkafkaError(-184, 'Local: Queue full'),
      new KafkaJSProtocolErrorLike('Local: Fatal error', -150, 'FATAL'),
      Object.assign(new Error('read ECONNRESET'), { code: 'ECONNRESET' }),
      new Error('no code at all'),
      // Not an Error: never classified, whatever it carries.
      { code: 10, message: 'Broker: Message size too large' },
      'a thrown string',
    ];

    for (const error of retryable) {
      const label =
        error instanceof Error
          ? `${error.message} (${String((error as { code?: unknown }).code)})`
          : `a thrown ${typeof error}`;
      it(`rethrows anything else as it is, for the relay to retry: ${label}`, async () => {
        const { transport } = failingWith(error);

        await assert.rejects(transport.publish(outboxMessage(), live()), thrown => {
          assert.equal(thrown, error);
          return true;
        });
      });
    }

    it('names the topic it produced to, after toRecord', async () => {
      const error = librdkafkaError(10, 'Broker: Message size too large');
      const { producer } = createMockKafkaProducer();
      const transport = new KafkaOutboxTransport(
        new KafkaProducerService({
          ...producer,
          send: async () => {
            throw error;
          },
        }),
        { toRecord: ({ topic }) => ({ topic: `staging.${topic}` }) },
      );

      await assert.rejects(
        transport.publish(outboxMessage(), live()),
        /for topic "staging\.orders\.placed" permanently/,
      );
    });

    it('classifies only Errors with a listed numeric code', () => {
      assert.equal(toPermanentFailure(librdkafkaError(29, 'denied'), 't') instanceof NonRetryableMessageError, true);
      assert.equal(toPermanentFailure(librdkafkaError(30, 'group denied'), 't'), undefined);
      assert.equal(toPermanentFailure(Object.assign(new Error('x'), { code: '10' }), 't'), undefined);
      assert.equal(toPermanentFailure({ code: 10 }, 't'), undefined);
      assert.equal(toPermanentFailure(undefined, 't'), undefined);
    });
  });

  describe('with()', () => {
    it('returns a subclass that builds the transport with the options', async () => {
      const { producer, calls } = createMockKafkaProducer();
      const Configured = KafkaOutboxTransport.with({
        toRecord: ({ topic }) => ({ topic: `staging.${topic}` }),
      });

      const transport = new Configured(new KafkaProducerService(producer));
      await transport.publish(outboxMessage(), live());

      assert.equal(transport instanceof KafkaOutboxTransport, true);
      assert.equal(transport instanceof OutboxTransport, true);
      assert.equal(calls.send[0].topic, 'staging.orders.placed');
    });

    it('returns a class of its own on every call', () => {
      const options: KafkaOutboxTransportOptions = {};
      assert.notEqual(KafkaOutboxTransport.with(options), KafkaOutboxTransport.with(options));
      assert.notEqual(KafkaOutboxTransport.with(options), KafkaOutboxTransport);
    });
  });
});
