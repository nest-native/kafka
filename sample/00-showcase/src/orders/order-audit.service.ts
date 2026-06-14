import { Injectable, Scope } from '@nestjs/common';
import { MessageLog } from '../common/message-log.service';

/**
 * A request-scoped provider: NestJS creates a fresh instance for every consumed
 * message, so each message gets its own audit id. This proves request scoping
 * works on the Kafka transport.
 */
@Injectable({ scope: Scope.REQUEST })
export class OrderAuditService {
  private static counter = 0;
  private readonly id = `audit-${(OrderAuditService.counter += 1)}`;

  constructor(private readonly log: MessageLog) {}

  audit(): void {
    this.log.recordAudit(this.id);
  }
}
