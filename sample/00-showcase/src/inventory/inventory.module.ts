import { Module } from '@nestjs/common';
import { InventoryConsumer } from './inventory.consumer';

/**
 * Hosts the replying handler. Nothing here opts into request-reply beyond the
 * handler's own `reply: true` — a replier learns its address from the request's
 * headers, so an application migrating only its `@MessagePattern` handlers adds
 * the flag and nothing else.
 */
@Module({ providers: [InventoryConsumer] })
export class InventoryModule {}
