import { Injectable } from '@nestjs/common';

/**
 * A singleton sink that records what each part of the showcase did, so the smoke
 * test can assert end-to-end behaviour (producer → consumer → derived event →
 * second consumer) without a real broker.
 */
@Injectable()
export class MessageLog {
  readonly handledOrders: string[] = [];
  readonly notifications: string[] = [];
  readonly pipeline: string[] = [];
  readonly auditedBy: string[] = [];

  record(channel: 'handledOrders' | 'notifications' | 'pipeline', value: string): void {
    this[channel].push(value);
  }

  recordAudit(id: string): void {
    this.auditedBy.push(id);
  }

  reset(): void {
    this.handledOrders.length = 0;
    this.notifications.length = 0;
    this.pipeline.length = 0;
    this.auditedBy.length = 0;
  }
}
