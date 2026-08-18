/**
 * IndexDO pagination-contract tests: cursor = last returned key and
 * list_complete reflects remaining keys. Pages also carry stored values so
 * run listing does not need one follow-up RPC per key.
 */
import { beforeEach, describe, expect, it } from 'vitest';
import { IndexDO } from '../src/worker/durable-objects/IndexDO.js';
import { FakeFleet } from '../src/testing/fake-cell.js';

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
});
