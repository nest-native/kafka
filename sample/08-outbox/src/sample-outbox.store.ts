import { Injectable } from '@nestjs/common';
import { InMemoryOutboxStore, OutboxStorage } from '@nestjs/outbox';

/**
 * The stand-in for your database.
 *
 * In production the outbox lives in the database your business data lives in,
 * through `PostgresOutboxStore` or `MySqlOutboxStore`, and `Outbox.add(tx, ...)`
 * takes your ORM's transaction. This sample has no database, so it registers
 * the outbox's own in-memory store the way a real store registers itself, and
 * uses its `transaction()`: the writes inside apply when it resolves and are
 * dropped when it throws, like a rollback.
 */
@Injectable()
export class SampleOutboxStore extends InMemoryOutboxStore {
  constructor(storage: OutboxStorage) {
    super();
    storage.registerSource({ messages: this, inbox: this });
  }
}
