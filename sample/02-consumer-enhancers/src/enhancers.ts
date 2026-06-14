import {
  ArgumentsHost,
  BadRequestException,
  CallHandler,
  CanActivate,
  Catch,
  ExceptionFilter,
  ExecutionContext,
  Injectable,
  Logger,
  NestInterceptor,
  PipeTransform,
} from '@nestjs/common';
import { Observable, tap } from 'rxjs';

/**
 * A record of everything the enhancer pipeline did for the most recent message,
 * so the smoke test can assert the exact order guards, interceptors, pipes, and
 * filters ran in.
 */
@Injectable()
export class PipelineTrace {
  readonly events: string[] = [];

  record(event: string): void {
    this.events.push(event);
  }

  reset(): void {
    this.events.length = 0;
  }
}

/**
 * Rejects messages whose payload is missing a `tenant`, proving guards run
 * before the handler on the Kafka transport.
 */
@Injectable()
export class TenantGuard implements CanActivate {
  constructor(private readonly trace: PipelineTrace) {}

  canActivate(context: ExecutionContext): boolean {
    this.trace.record('guard');
    const message = context.switchToRpc().getData<{ tenant?: string }>();
    return typeof message?.tenant === 'string' && message.tenant.length > 0;
  }
}

/**
 * Wraps handler execution, recording before/after so interceptor ordering is
 * observable.
 */
@Injectable()
export class TimingInterceptor implements NestInterceptor {
  constructor(private readonly trace: PipelineTrace) {}

  intercept(_context: ExecutionContext, next: CallHandler): Observable<unknown> {
    this.trace.record('interceptor:before');
    return next
      .handle()
      .pipe(tap(() => this.trace.record('interceptor:after')));
  }
}

/**
 * Normalises the payload before the handler sees it.
 */
@Injectable()
export class NormalizeOrderPipe implements PipeTransform {
  constructor(private readonly trace: PipelineTrace) {}

  transform(value: { id?: string; tenant?: string }): {
    id: string;
    tenant: string;
  } {
    this.trace.record('pipe');
    if (!value?.id) {
      throw new BadRequestException('order id is required');
    }
    return { id: value.id, tenant: value.tenant ?? 'unknown' };
  }
}

/**
 * Catches `BadRequestException`s thrown by the pipe or handler and records them
 * instead of letting the transport rethrow.
 */
@Catch(BadRequestException)
export class BadRequestTraceFilter implements ExceptionFilter {
  private readonly logger = new Logger(BadRequestTraceFilter.name);

  constructor(private readonly trace: PipelineTrace) {}

  catch(exception: BadRequestException, _host: ArgumentsHost): string {
    this.trace.record(`filter:${exception.message}`);
    this.logger.warn(`Suppressed bad request: ${exception.message}`);
    return 'handled';
  }
}
