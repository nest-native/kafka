import { Injectable } from '@nestjs/common';

/**
 * A singleton sink that records what each part of the showcase did, so the smoke
 * test can assert end-to-end behaviour (producer → consumer → derived event →
 * second consumer, plus batch aggregation) without a real broker.
 */
@Injectable()
export class MessageLog {
  readonly handledOrders: string[] = [];
  readonly notifications: string[] = [];
  readonly pipeline: string[] = [];
  readonly auditedBy: string[] = [];

  /** One entry per aggregated batch: the partition and how many events it held. */
  readonly batches: { partition: number; count: number }[] = [];

  record(channel: 'handledOrders' | 'notifications' | 'pipeline', value: string): void {
    this[channel].push(value);
  }

  recordAudit(id: string): void {
    this.auditedBy.push(id);
  }

  recordBatch(partition: number, count: number): void {
    this.batches.push({ partition, count });
  }

  reset(): void {
    this.handledOrders.length = 0;
    this.notifications.length = 0;
    this.pipeline.length = 0;
    this.auditedBy.length = 0;
    this.batches.length = 0;
  }
}
