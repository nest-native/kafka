import { Injectable, Logger } from '@nestjs/common';

/**
 * The "one handler that logs messages" from milestone 2.
 *
 * In this milestone the full `@KafkaConsumer` / `@KafkaHandler` decorator
 * pipeline has not landed yet, so the in-memory driver in `kafka-driver.ts`
 * delivers every produced message straight to this handler. It logs each
 * message and keeps a record so the smoke test can assert delivery.
 */
@Injectable()
export class LoggingMessageHandler {
  private readonly logger = new Logger(LoggingMessageHandler.name);
  private readonly received: Array<{ topic: string; value: string }> = [];

  handle(topic: string, value: string): void {
    this.received.push({ topic, value });
    this.logger.log(`Received message on "${topic}": ${value}`);
  }

  getReceived(): ReadonlyArray<{ topic: string; value: string }> {
    return this.received;
  }
}
