import {
  ArgumentsHost,
  BadRequestException,
  CallHandler,
  CanActivate,
  Catch,
  ExceptionFilter,
  ExecutionContext,
  Injectable,
  NestInterceptor,
  PipeTransform,
} from '@nestjs/common';
import { Observable, tap } from 'rxjs';
import { MessageLog } from './message-log.service';

/**
 * Rejects orders that carry no tenant — a guard running on the Kafka transport
 * before the handler, just like `@UseGuards` on an HTTP controller.
 */
@Injectable()
export class TenantGuard implements CanActivate {
  constructor(private readonly log: MessageLog) {}

  canActivate(context: ExecutionContext): boolean {
    this.log.record('pipeline', 'guard');
    const data = context.switchToRpc().getData<{ tenant?: string }>();
    return typeof data?.tenant === 'string' && data.tenant.length > 0;
  }
}

/**
 * Records before/after around the handler to make interceptor ordering visible.
 */
@Injectable()
export class TimingInterceptor implements NestInterceptor {
  constructor(private readonly log: MessageLog) {}

  intercept(_context: ExecutionContext, next: CallHandler): Observable<unknown> {
    this.log.record('pipeline', 'interceptor:before');
    return next
      .handle()
      .pipe(tap(() => this.log.record('pipeline', 'interceptor:after')));
  }
}

/**
 * Validates and normalises the order payload before the handler runs.
 */
@Injectable()
export class OrderValidationPipe implements PipeTransform {
  constructor(private readonly log: MessageLog) {}

  transform(value: { id?: string; tenant?: string; amount?: number }): {
    id: string;
    tenant: string;
    amount: number;
  } {
    this.log.record('pipeline', 'pipe');
    if (!value?.id) {
      throw new BadRequestException('order id is required');
    }
    return {
      id: value.id,
      tenant: value.tenant ?? 'unknown',
      amount: value.amount ?? 0,
    };
  }
}

/**
 * Catches `BadRequestException`s so a malformed message is acknowledged instead
 * of bubbling up to the transport.
 */
@Catch(BadRequestException)
export class OrderErrorFilter implements ExceptionFilter {
  constructor(private readonly log: MessageLog) {}

  catch(exception: BadRequestException, _host: ArgumentsHost): string {
    this.log.record('pipeline', `filter:${exception.message}`);
    return 'handled';
  }
}
