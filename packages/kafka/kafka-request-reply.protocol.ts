import {
  KafkaMessageHeaders,
  KafkaProducerMessage,
  KafkaRecordMetadata,
  KafkaSendRecord,
} from './driver';
import {
  KafkaRequestReplyHeaderKeys,
  KafkaRequestReplyOptions,
} from './interfaces';

/**
 * The wire protocol shared by both sides of request-reply: which headers carry
 * the address, how a reply message is built, and how the module options resolve
 * to their defaults.
 *
 * Everything here is a pure function over headers so both sides — the requester
 * in {@link KafkaRequestReplyService} and the replier in
 * {@link KafkaReplyPublisher} — read and write exactly the same bytes, and so
 * the protocol is testable without a broker or a Nest application.
 *
 * @internal
 */

/**
 * `@nestjs/microservices`' own keys, verified against the installed
 * `KafkaHeaders` enum. They are the defaults because the realistic migration is
 * partial — service A moves to this package while B and C stay on the official
 * transport — and these keys make both directions work with no configuration on
 * either side.
 *
 * @publicApi
 */
export const DEFAULT_KAFKA_REQUEST_REPLY_HEADERS: KafkaRequestReplyHeaderKeys =
  Object.freeze({
    correlationId: 'kafka_correlationId',
    replyTopic: 'kafka_replyTopic',
    replyPartition: 'kafka_replyPartition',
    error: 'kafka_nest-err',
    disposed: 'kafka_nest-is-disposed',
  });

/** Default per-request timeout. */
export const DEFAULT_REQUEST_TIMEOUT_MS = 30000;

/** Default bound on how long `request()` waits for reply-consumer readiness. */
export const DEFAULT_READINESS_TIMEOUT_MS = 10000;

/**
 * Marks the readiness sentinel this package produces onto its own reply topic
 * at startup. It exists purely so an operator reading the reply topic can tell
 * what the valueless message is; nothing branches on it.
 */
export const KAFKA_READINESS_PROBE_HEADER = 'nest-native-kafka-readiness';

/**
 * The producer surface both sides of request-reply need: exactly
 * {@link KafkaProducerService.send}, narrowed to the one method so the reply
 * consumer and the reply publisher stay unit-testable without a Nest container.
 * Every request, reply, and readiness sentinel goes through the module's shared
 * producer, which is what keeps `KafkaTestModule` able to see them and graceful
 * shutdown ordering unchanged.
 */
export interface KafkaReplySender {
  send(record: KafkaSendRecord): Promise<KafkaRecordMetadata[]>;
}

/**
 * The module options resolved against their defaults, so no consumer of the
 * configuration has to repeat a `??`.
 */
export interface ResolvedRequestReplyOptions {
  replyTopic: string;
  timeoutMs: number;
  readinessTimeoutMs: number;
  groupIdPrefix: string;
  headers: KafkaRequestReplyHeaderKeys;
  consumer: Record<string, unknown>;
}

export function resolveRequestReplyOptions(
  options: KafkaRequestReplyOptions,
): ResolvedRequestReplyOptions {
  return {
    replyTopic: options.replyTopic,
    timeoutMs: options.timeoutMs ?? DEFAULT_REQUEST_TIMEOUT_MS,
    readinessTimeoutMs:
      options.readinessTimeoutMs ?? DEFAULT_READINESS_TIMEOUT_MS,
    groupIdPrefix: options.groupIdPrefix ?? `${options.replyTopic}-`,
    headers: resolveHeaderKeys(options.headers),
    consumer: options.consumer ?? {},
  };
}

/**
 * Merge caller overrides onto the interoperable defaults. Overriding a single
 * key is supported: teams on a green field can rename the ones they dislike and
 * lose nothing but interop they did not need.
 */
export function resolveHeaderKeys(
  overrides?: Partial<KafkaRequestReplyHeaderKeys>,
): KafkaRequestReplyHeaderKeys {
  return { ...DEFAULT_KAFKA_REQUEST_REPLY_HEADERS, ...overrides };
}

/**
 * Read one header as text.
 *
 * Kafka headers are bytes, and the client models a repeated header as an array,
 * so a value can arrive as a `Buffer`, a `string`, or an array of either. The
 * protocol is presence-based everywhere, which is what lets it tolerate both
 * this package's replies and `@nestjs/microservices`'.
 */
export function readHeaderText(
  headers: KafkaMessageHeaders | undefined,
  key: string,
): string | undefined {
  const raw = headers?.[key];
  if (raw === undefined) {
    return undefined;
  }
  if (Array.isArray(raw)) {
    return raw.length === 0 ? undefined : decodeHeader(raw[0]);
  }
  return decodeHeader(raw);
}

function decodeHeader(value: Buffer | string): string {
  return Buffer.isBuffer(value) ? value.toString('utf8') : value;
}

/**
 * Where a consumed request asked to be answered.
 */
export interface KafkaReplyAddress {
  topic: string;
  correlationId: string;
  /**
   * Set only when the request advertised one — an un-migrated `ClientKafka`
   * always does, because it throws client-side without a partition it owns.
   */
  partition?: number;
}

/**
 * The three things a consumed message's headers can say about replying.
 */
export type KafkaReplyAddressResult =
  | { status: 'none' }
  | { status: 'undeliverable'; detail: string }
  | { status: 'ok'; address: KafkaReplyAddress };

/**
 * Resolve the reply address from a consumed request's headers.
 *
 * A message is a request **iff** it carries both a correlation id and a reply
 * topic — the same rule `ServerKafka` applies, so a `reply: true` handler and an
 * `@MessagePattern` handler classify identical traffic identically.
 *
 * A reply partition that is present but not a non-negative integer makes the
 * request statically undeliverable: no redelivery can ever fix a garbage
 * address, so retrying it would only block the partition behind a poison
 * request.
 */
export function resolveReplyAddress(
  headers: KafkaMessageHeaders | undefined,
  keys: KafkaRequestReplyHeaderKeys,
): KafkaReplyAddressResult {
  const correlationId = readHeaderText(headers, keys.correlationId);
  const topic = readHeaderText(headers, keys.replyTopic);
  if (correlationId === undefined || topic === undefined) {
    return { status: 'none' };
  }

  const rawPartition = readHeaderText(headers, keys.replyPartition);
  if (rawPartition === undefined) {
    return { status: 'ok', address: { topic, correlationId } };
  }

  const partition = Number(rawPartition);
  if (!Number.isInteger(partition) || partition < 0) {
    return {
      status: 'undeliverable',
      detail:
        `Kafka request (correlation id "${correlationId}") advertised the ` +
        `unparseable reply partition "${rawPartition}" for topic "${topic}". ` +
        'The reply is skipped and the offset committed: no redelivery can ' +
        'make an undeliverable address deliverable.',
    };
  }
  return { status: 'ok', address: { topic, correlationId, partition } };
}

/**
 * A replying handler's outcome: the value it returned, or the error that escaped
 * it and mapped to `'commit'`.
 */
export type KafkaReplyOutcome =
  | { status: 'value'; value: unknown }
  | { status: 'error'; error: unknown };

/**
 * Build the reply message for one request.
 *
 * The key is the correlation id (deterministic and greppable). The headers carry
 * the correlation id, the completion marker — which is what makes an
 * un-migrated `ClientKafka`'s observable emit and complete on this single reply
 * — and, on the error path, the error header. Request headers are deliberately
 * *not* echoed: propagating tracing context is an interceptor's job, per the
 * package's header neutrality.
 */
export function buildReplyMessage(
  address: KafkaReplyAddress,
  keys: KafkaRequestReplyHeaderKeys,
  outcome: KafkaReplyOutcome,
): KafkaProducerMessage {
  const headers: KafkaMessageHeaders = {
    [keys.correlationId]: address.correlationId,
    // `ServerKafka` writes a one-byte buffer here and every reader is
    // presence-based; mirroring the bytes keeps the interop claim literal.
    [keys.disposed]: Buffer.alloc(1),
  };

  let value: Buffer | string | null = null;
  if (outcome.status === 'error') {
    headers[keys.error] = describeReplyError(outcome.error);
  } else {
    value = serializeReplyValue(outcome.value);
  }

  const message: KafkaProducerMessage = {
    key: address.correlationId,
    value,
    headers,
  };
  if (address.partition !== undefined) {
    message.partition = address.partition;
  }
  return message;
}

/**
 * Serialize a handler's return value for the reply.
 *
 * The producer API stays explicit-serialization everywhere else; the transport
 * serializes here because a handler has no seam to do it itself. `string`,
 * `Buffer`, and `null` pass through, `undefined` becomes a `null` value, and
 * everything else is JSON — including the values `JSON.stringify` refuses to
 * represent (a function, a symbol), which become `null` rather than a broken
 * message.
 */
export function serializeReplyValue(value: unknown): Buffer | string | null {
  if (value === undefined || value === null) {
    return null;
  }
  if (typeof value === 'string' || Buffer.isBuffer(value)) {
    return value;
  }
  return JSON.stringify(value) ?? null;
}

/**
 * Encode a failed handler's error for the error header. `{ name, message }`
 * only: an error reply crosses a process boundary to a caller that cannot act
 * on a stack trace, and stacks leak internals.
 */
export function describeReplyError(error: unknown): string {
  if (error instanceof Error) {
    return JSON.stringify({ name: error.name, message: error.message });
  }
  return JSON.stringify({ name: 'Error', message: String(error) });
}

/**
 * Reject request headers that collide with the protocol's own keys instead of
 * silently overwriting them — a caller stamping its own `kafka_correlationId`
 * is either fighting the transport or has a bug, and both deserve to be told.
 */
export function assertNoReservedHeaders(
  headers: KafkaMessageHeaders | undefined,
  keys: KafkaRequestReplyHeaderKeys,
): void {
  const reserved = [keys.correlationId, keys.replyTopic, keys.replyPartition];
  for (const key of reserved) {
    if (headers?.[key] !== undefined) {
      throw new Error(
        `The Kafka request header "${key}" is reserved by request-reply and ` +
          'cannot be set by the caller. Remove it, or rename the key through ' +
          '"requestReply.headers".',
      );
    }
  }
}
