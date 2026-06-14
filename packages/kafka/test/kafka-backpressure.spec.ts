import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { createBackpressure } from '../kafka-backpressure';

/**
 * Deferred promise helper: a task the test resolves by hand so it can observe how
 * many tasks the semaphore lets run concurrently.
 */
function deferred(): { promise: Promise<void>; resolve: () => void } {
  let resolve: () => void = () => {};
  const promise = new Promise<void>(r => {
    resolve = r;
  });
  return { promise, resolve };
}

describe('createBackpressure', () => {
  it('runs tasks immediately when uncapped (maxInFlight <= 0)', async () => {
    const limiter = createBackpressure(0);
    let running = 0;
    let peak = 0;

    const tasks = [0, 1, 2].map(() =>
      limiter.run(async () => {
        running += 1;
        peak = Math.max(peak, running);
        await Promise.resolve();
        running -= 1;
      }),
    );
    await Promise.all(tasks);

    // Uncapped: all three were allowed to start before any finished.
    assert.equal(peak, 3);
  });

  it('treats a negative permit count as uncapped', async () => {
    const limiter = createBackpressure(-5);
    const result = await limiter.run(async () => 'ok');
    assert.equal(result, 'ok');
  });

  it('caps the number of concurrently running tasks', async () => {
    const limiter = createBackpressure(2);
    let running = 0;
    let peak = 0;
    const gates = [deferred(), deferred(), deferred(), deferred()];

    const tasks = gates.map(gate =>
      limiter.run(async () => {
        running += 1;
        peak = Math.max(peak, running);
        await gate.promise;
        running -= 1;
      }),
    );

    // Let the scheduler start whatever it can; only two may run at once.
    await Promise.resolve();
    await Promise.resolve();
    assert.equal(peak, 2);

    // Releasing one permit lets exactly one waiter through.
    gates[0].resolve();
    await Promise.resolve();
    await Promise.resolve();
    assert.equal(peak, 2);

    gates[1].resolve();
    gates[2].resolve();
    gates[3].resolve();
    await Promise.all(tasks);
    assert.equal(running, 0);
  });

  it('releases the permit even when a task rejects, freeing a waiter', async () => {
    const limiter = createBackpressure(1);
    const order: string[] = [];
    const gate = deferred();
    let secondStarted = false;

    const failing = limiter
      .run(async () => {
        order.push('start-failing');
        await gate.promise;
        throw new Error('boom');
      })
      .catch(() => order.push('caught-failing'));

    const following = limiter.run(async () => {
      secondStarted = true;
      order.push('start-following');
    });

    // While the first task holds the only permit the second cannot start.
    await Promise.resolve();
    await Promise.resolve();
    assert.equal(secondStarted, false, 'the second task waits for the permit');
    assert.deepEqual(order, ['start-failing']);

    // Rejecting the first task must still release the permit for the waiter.
    gate.resolve();
    await Promise.all([failing, following]);

    assert.equal(secondStarted, true);
    assert.deepEqual(order.slice().sort(), [
      'caught-failing',
      'start-failing',
      'start-following',
    ]);
  });
});
