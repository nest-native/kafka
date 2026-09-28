import { KafkaMessageHeaders, KafkaProducerMessage } from './driver';
import {
  KafkaBatchContext,
  KafkaContext,
  KafkaIncomingMessage,
} from './kafka-context';

/**
 * The headers a dead-letter record carries. Names and encodings are Spring
 * Kafka's (`KafkaHeaders.DLT_*`, written by its `DeadLetterPublishingRecoverer`):
 * strings are UTF-8, the partition is a 4-byte big-endian integer, and the
 * offset and timestamp are 8-byte big-endian integers. A dead-letter topic
 * written by this package therefore reads the same as one written by a JVM
 * service, and Spring tooling can read ours.
 */
export const KAFKA_DEAD_LETTER_HEADERS = Object.freeze({
  originalTopic: 'kafka_dlt-original-topic',
  originalPartition: 'kafka_dlt-original-partition',
  originalOffset: 'kafka_dlt-original-offset',
  originalTimestamp: 'kafka_dlt-original-timestamp',
  originalConsumerGroup: 'kafka_dlt-original-consumer-group',
  exceptionFqcn: 'kafka_dlt-exception-fqcn',
  exceptionMessage: 'kafka_dlt-exception-message',
  exceptionStacktrace: 'kafka_dlt-exception-stacktrace',
} as const);

/** Options for {@link toDeadLetterMessage} and {@link toDeadLetterMessages}. */
export interface KafkaDeadLetterOptions {
  /** Record the consumer group that gave up on the message. */
  consumerGroup?: string;

  /**
   * Include the error's stack trace, as Spring does. A stack trace names files
   * and code paths; turn it off when the dead-letter topic is readable outside
   * the team that owns the code.
   *
   * @default true
   */
  includeStackTrace?: boolean;
}

/** The metadata of a dead-letter record, decoded by {@link readDeadLetterHeaders}. */
export interface KafkaDeadLetterInfo {
  originalTopic: string;
  originalPartition?: number;
  /** A string, like every offset in this package: an int64 can exceed `Number`. */
  originalOffset?: string;
  /** Milliseconds since the epoch, as a string, like `KafkaIncomingMessage.timestamp`. */
  originalTimestamp?: string;
  originalConsumerGroup?: string;
  /** The error's class name — `BadRequestException`, `TypeError`, … */
  exceptionFqcn?: string;
  exceptionMessage?: string;
  exceptionStacktrace?: string;
}

/**
 * Build the dead-letter record for a message a handler failed on: the original
 * key, value, and headers, plus the {@link KAFKA_DEAD_LETTER_HEADERS} describing
 * where it came from and why it failed. Produce it wherever the dead letters of
 * that topic live — the package does not pick the topic or produce for you.
 *
 * Dead-letter the message *before* it is committed, and only once the produce
 * succeeded — from an exception filter that awaits the produce, or from an
 * async error mapper that returns `'commit'` afterwards. Both are awaited, and
 * a failed produce leaves the message to be retried.
 *
 * @example
 * ```ts
 * @Catch(BadRequestException)
 * export class DeadLetterFilter implements ExceptionFilter {
 *   constructor(private readonly producer: KafkaProducerService) {}
 *
 *   async catch(error: BadRequestException, host: ArgumentsHost): Promise<void> {
 *     const context = host.switchToRpc().getContext<KafkaContext>();
 *     await this.producer.send({
 *       topic: `${context.getTopic()}.dlq`,
 *       messages: [toDeadLetterMessage(context, error)],
 *     });
 *   }
 * }
 * ```
 */
export function toDeadLetterMessage(
  context: KafkaContext,
  error: unknown,
  options: KafkaDeadLetterOptions = {},
): KafkaProducerMessage {
  return deadLetter(
    context.getTopic(),
    context.getPartition(),
    context.getMessage(),
    error,
    options,
  );
}

/**
 * {@link toDeadLetterMessage} for a batch handler: one dead-letter record per
 * message of the batch, in order. A batch fails as a whole, so every message in
 * it carries the same error.
 */
export function toDeadLetterMessages(
  context: KafkaBatchContext,
  error: unknown,
  options: KafkaDeadLetterOptions = {},
): KafkaProducerMessage[] {
  const batch = context.getBatch();
  return batch.messages.map(message =>
    deadLetter(batch.topic, batch.partition, message, error, options),
  );
}

/**
 * Decode the {@link KAFKA_DEAD_LETTER_HEADERS} of a consumed dead-letter record,
 * or `undefined` when it carries no original topic — it is not a dead letter.
 * Accepts the binary encodings Spring writes and, for the numeric fields, plain
 * decimal strings, so dead letters from other producers decode too.
 */
export function readDeadLetterHeaders(
  headers: KafkaMessageHeaders | undefined,
): KafkaDeadLetterInfo | undefined {
  const text = (name: string): string | undefined => {
    const value = lastValue(headers?.[name]);
    return value === undefined ? undefined : value.toString('utf8');
  };
  const originalTopic = text(KAFKA_DEAD_LETTER_HEADERS.originalTopic);
  if (originalTopic === undefined) {
    return undefined;
  }
  return {
    originalTopic,
    originalPartition: readInt32(headers, KAFKA_DEAD_LETTER_HEADERS.originalPartition),
    originalOffset: readInt64(headers, KAFKA_DEAD_LETTER_HEADERS.originalOffset),
    originalTimestamp: readInt64(headers, KAFKA_DEAD_LETTER_HEADERS.originalTimestamp),
    originalConsumerGroup: text(KAFKA_DEAD_LETTER_HEADERS.originalConsumerGroup),
    exceptionFqcn: text(KAFKA_DEAD_LETTER_HEADERS.exceptionFqcn),
    exceptionMessage: text(KAFKA_DEAD_LETTER_HEADERS.exceptionMessage),
    exceptionStacktrace: text(KAFKA_DEAD_LETTER_HEADERS.exceptionStacktrace),
  };
}

function deadLetter(
  topic: string,
  partition: number,
  message: KafkaIncomingMessage,
  error: unknown,
  options: KafkaDeadLetterOptions,
): KafkaProducerMessage {
  const headers: KafkaMessageHeaders = { ...message.headers };
  const names = KAFKA_DEAD_LETTER_HEADERS;
  headers[names.originalTopic] = topic;
  headers[names.originalPartition] = int32(partition);
  setIfDefined(headers, names.originalOffset, int64(message.offset));
  setIfDefined(headers, names.originalTimestamp, int64(message.timestamp));
  setIfDefined(headers, names.originalConsumerGroup, options.consumerGroup);
  headers[names.exceptionFqcn] = errorClassName(error);
  headers[names.exceptionMessage] = error instanceof Error ? error.message : String(error);
  if (options.includeStackTrace !== false && error instanceof Error && error.stack) {
    headers[names.exceptionStacktrace] = error.stack;
  }

  const record: KafkaProducerMessage = { value: message.value, headers };
  if (message.key !== undefined) {
    record.key = message.key;
  }
  return record;
}

function errorClassName(error: unknown): string {
  if (error instanceof Error) {
    return error.constructor.name;
  }
  return error === null ? 'null' : typeof error;
}

function setIfDefined(
  headers: KafkaMessageHeaders,
  name: string,
  value: Buffer | string | undefined,
): void {
  if (value !== undefined) {
    headers[name] = value;
  }
}

function int32(value: number): Buffer {
  const buffer = Buffer.alloc(4);
  buffer.writeInt32BE(value);
  return buffer;
}

/** An int64 header for a decimal-string offset or timestamp; none when absent or not an integer. */
function int64(value: string | undefined): Buffer | undefined {
  if (value === undefined || !/^-?\d+$/.test(value)) {
    return undefined;
  }
  const buffer = Buffer.alloc(8);
  buffer.writeBigInt64BE(BigInt(value));
  return buffer;
}

function lastValue(
  value: KafkaMessageHeaders[string],
): Buffer | undefined {
  const last = Array.isArray(value) ? value[value.length - 1] : value;
  if (last === undefined) {
    return undefined;
  }
  return Buffer.isBuffer(last) ? last : Buffer.from(last, 'utf8');
}

function readInt32(
  headers: KafkaMessageHeaders | undefined,
  name: string,
): number | undefined {
  const value = lastValue(headers?.[name]);
  if (value === undefined) {
    return undefined;
  }
  const text = decimalText(value);
  if (text !== undefined) {
    return Number(text);
  }
  return value.length === 4 ? value.readInt32BE() : undefined;
}

function readInt64(
  headers: KafkaMessageHeaders | undefined,
  name: string,
): string | undefined {
  const value = lastValue(headers?.[name]);
  if (value === undefined) {
    return undefined;
  }
  return (
    decimalText(value) ??
    (value.length === 8 ? value.readBigInt64BE().toString() : undefined)
  );
}

/**
 * A header another producer wrote as a decimal string. Checked before the
 * binary reading: a value that is all ASCII digits is text, since a real binary
 * partition, offset, or timestamp starts with a zero byte, and "12345678" would
 * otherwise read as an 8-byte integer.
 */
function decimalText(value: Buffer): string | undefined {
  const text = value.toString('latin1');
  return /^-?\d+$/.test(text) ? text : undefined;
}
