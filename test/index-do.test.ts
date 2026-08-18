/**
 * IndexDO pagination-contract tests: cursor = last returned key and
 * list_complete reflects remaining keys. Pages also carry stored values so
 * run listing does not need one follow-up RPC per key.
 */
import { beforeEach, describe, expect, it } from 'vitest';
import { IndexDO } from '../src/worker/durable-objects/IndexDO.js';
import { FakeFleet } from '../src/testing/fake-cell.js';
import { stringify } from '../src/vendor/shared/index.js';

describe('IndexDO', () => {
  let fleet: FakeFleet;
  let index: IndexDO;

  beforeEach(() => {
    fleet = new FakeFleet({ index: IndexDO as never });
    index = fleet.namespace('index').get({ toString: () => 'index' }) as IndexDO;
  });

  it('get/put/delete round-trip', async () => {
    expect(await index.get('run:wf:1')).toBeNull();
    await index.put('run:wf:1', '{"runId":"wrun_1"}');
    expect(await index.get('run:wf:1')).toBe('{"runId":"wrun_1"}');
    await index.delete('run:wf:1');
    expect(await index.get('run:wf:1')).toBeNull();
  });

  it('lists with prefix filtering in lexicographic order', async () => {
    await index.put('run:a:1', 'x');
    await index.put('run:a:2', 'x');
    await index.put('run:b:1', 'x');
    await index.put('hook:t1', 'x');

    const all = await index.list({ prefix: 'run:' });
    expect(all.keys).toEqual([
      { name: 'run:a:1', value: 'x' },
      { name: 'run:a:2', value: 'x' },
      { name: 'run:b:1', value: 'x' },
    ]);
    expect(all.list_complete).toBe(true);
    expect(all.cursor).toBeUndefined();
  });

  it('paginates with exact limit + list_complete (no dropped keys)', async () => {
    for (let i = 1; i <= 5; i++) {
      await index.put(`run:wf:${i}`, String(i));
    }

    const page1 = await index.list({ prefix: 'run:', limit: 2 });
    expect(page1.keys).toEqual([
      { name: 'run:wf:1', value: '1' },
      { name: 'run:wf:2', value: '2' },
    ]);
    expect(page1.list_complete).toBe(false);
    expect(page1.cursor).toBe('run:wf:2');

    const page2 = await index.list({ prefix: 'run:', limit: 2, cursor: page1.cursor });
    expect(page2.keys).toEqual([
      { name: 'run:wf:3', value: '3' },
      { name: 'run:wf:4', value: '4' },
    ]);
    expect(page2.list_complete).toBe(false);

    const page3 = await index.list({ prefix: 'run:', limit: 2, cursor: page2.cursor });
    expect(page3.keys).toEqual([{ name: 'run:wf:5', value: '5' }]);
    expect(page3.list_complete).toBe(true);
    expect(page3.cursor).toBeUndefined();
  });

  it('reports list_complete=true when the page ends exactly at the last key', async () => {
    await index.put('run:wf:1', 'x');
    await index.put('run:wf:2', 'x');

    const page = await index.list({ prefix: 'run:', limit: 2 });
    expect(page.keys).toHaveLength(2);
    expect(page.list_complete).toBe(true);
    expect(page.cursor).toBeUndefined();
  });

  it('caps direct index listings at the public pagination limit', async () => {
    const storage = fleet.cell('index', 'index').storage;
    for (let item = 0; item < 1_001; item++) {
      storage.data.set(`run:bounded:${String(item).padStart(4, '0')}`, String(item));
    }

    const page = await index.list({ prefix: 'run:bounded:', limit: 10_000 });
    expect(page.keys).toHaveLength(1_000);
    expect(page.list_complete).toBe(false);
    expect(page.cursor).toBe('run:bounded:0999');
  });

  it('atomically deletes owned indexes and fences delayed publication', async () => {
    await index.putOwned('wrun_expired', 'run:wf:1:wrun_expired', 'run');
    await index.putOwned('wrun_expired', 'correlation:c:1:e:wrun_expired', 'event');

    expect(
      await index.expireRun({
        runId: 'wrun_expired',
        keys: ['run:wf:1:wrun_expired', 'correlation:c:1:e:wrun_expired'],
        hooks: [],
        expiredAt: 123,
      }),
    ).toEqual({ deleted: 2 });
    expect(await index.get('run:wf:1:wrun_expired')).toBeNull();
    expect(await index.get('correlation:c:1:e:wrun_expired')).toBeNull();
    expect(await index.putOwned('wrun_expired', 'run:wf:2:wrun_expired', 'late')).toEqual({
      stored: false,
    });
    expect(await index.get('run:wf:2:wrun_expired')).toBeNull();
  });

  it('hides a hook immediately behind an expiration fence before its cleanup page', async () => {
    const runId = 'wrun_expired_hook';
    const owner = { runId, hookId: 'hook-expired' };
    const serialized = stringify({ ...owner, token: 'token-expired' });
    await index.finalizeHookIndexes('token-expired', owner.hookId, serialized, owner);

    await index.expireRun({ runId, keys: [], hooks: [], expiredAt: 123 });

    expect(fleet.cell('index', 'index').storage.data.has('hook:token-expired')).toBe(true);
    expect(await index.get('hook:token-expired')).toBeNull();
    expect(await index.get(`hookid:${owner.hookId}`)).toBeNull();
  });

  it('bounds hook cleanup reads and deletes to native 128-key batches', async () => {
    const runId = 'wrun_many_hooks';
    const keys = [`run:wf:1:${runId}`, `runall:1:${runId}`];
    for (const key of keys) await index.putOwned(runId, key, runId);
    const hooks = Array.from({ length: 64 }, (_, hookIndex) => ({
      hookId: `hook-${hookIndex}`,
      token: `token-${hookIndex}`,
    }));
    for (const hook of hooks) {
      const owner = { runId, hookId: hook.hookId };
      await index.finalizeHookIndexes(
        hook.token,
        hook.hookId,
        stringify({ ...owner, token: hook.token }),
        owner,
      );
    }
    const storage = fleet.cell('index', 'index').storage;
    storage.operationCalls.length = 0;

    expect(await index.expireRun({ runId, keys, hooks, expiredAt: 123 })).toEqual({
      deleted: 130,
    });
    expect(
      storage.operationCalls
        .filter((call) => call.operation === 'get')
        .map((call) => call.keys.length),
    ).toEqual([128, 64]);
    expect(
      storage.operationCalls
        .filter((call) => call.operation === 'delete')
        .map((call) => call.keys.length),
    ).toEqual([128, 3]);
  });

  it('batches terminal hook release and fences delayed finalization', async () => {
    const runId = 'wrun_terminal_hooks';
    const owner = { runId, hookId: 'hook-terminal' };
    await index.reserveHookToken('token-terminal', owner);

    expect(
      await index.releaseHookIndexes({
        runId,
        hooks: [{ hookId: owner.hookId, token: 'token-terminal' }],
        terminal: true,
      }),
    ).toEqual({ deleted: 1 });
    await index.finalizeHookIndexes(
      'token-terminal',
      owner.hookId,
      stringify({ ...owner, token: 'token-terminal' }),
      owner,
    );

    expect(await index.get(`terminal:${runId}`)).not.toBeNull();
    expect(await index.get('hook:token-terminal')).toBeNull();
    expect(await index.get(`hookid:${owner.hookId}`)).toBeNull();
    expect(await index.get('hookclaim:token-terminal')).toBeNull();
  });
});
