import type { WorkflowRun } from '@workflow/world';
import { describe, expect, it } from 'vitest';
import {
  HOOK_CREATED_KEY_PREFIX,
  HOOK_KEY_PREFIX,
  STEP_CREATED_KEY_PREFIX,
  STEP_KEY_PREFIX,
} from '../src/apply-event.js';
import { FakeFleet } from '../src/testing/fake-cell.js';
import { WorkflowRunDO } from '../src/worker/durable-objects/WorkflowRunDO.js';

function creationKey(prefix: string, index: number, id: string): string {
  return `${prefix}2026-01-01T00:${String(index).padStart(4, '0')}:00.000Z:${id}`;
}

describe('WorkflowRunDO hot-path storage operations', () => {
  it('reads a run and both retention fences in one batch operation', async () => {
    const fleet = new FakeFleet({ runs: WorkflowRunDO });
    const run = fleet.namespace('runs').get({ toString: () => 'wrun_batch' }) as WorkflowRunDO;
    const storage = fleet.cell('runs', 'wrun_batch').storage;
    storage.data.set('run', { runId: 'wrun_batch' } satisfies Partial<WorkflowRun>);
    storage.resetOperationCounts();

    expect(await run.getRun()).toMatchObject({ ok: true, value: { runId: 'wrun_batch' } });
    expect(storage.operationCounts.getMany).toBe(1);
    expect(storage.operationCounts.get).toBe(0);
    expect(storage.getManyCalls).toEqual([['retention:tombstone', 'retention:cleanup', 'run']]);
  });

  it.each([
    ['steps', STEP_CREATED_KEY_PREFIX, STEP_KEY_PREFIX, 'stepId'],
    ['hooks', HOOK_CREATED_KEY_PREFIX, HOOK_KEY_PREFIX, 'hookId'],
  ] as const)(
    'hydrates 100 %s with one bounded multi-get',
    async (kind, indexPrefix, entityPrefix, idKey) => {
      const fleet = new FakeFleet({ runs: WorkflowRunDO });
      const run = fleet.namespace('runs').get({ toString: () => `wrun_${kind}` }) as WorkflowRunDO;
      const storage = fleet.cell('runs', `wrun_${kind}`).storage;
      for (let index = 0; index < 100; index++) {
        const id = `${kind}-${String(index).padStart(3, '0')}`;
        storage.data.set(creationKey(indexPrefix, index, id), id);
        storage.data.set(`${entityPrefix}${id}`, { [idKey]: id });
      }
      storage.resetOperationCounts();
      storage.listCalls.length = 0;

      const result =
        kind === 'steps'
          ? await run.listSteps({ limit: 100 })
          : await run.listHooks({ limit: 100 });
      expect(result.ok && result.value.data).toHaveLength(100);
      expect(storage.listCalls).toHaveLength(1);
      expect(storage.operationCounts.getMany).toBe(2);
      expect(storage.operationCounts.get).toBe(0);
      expect(storage.getManyCalls).toEqual([
        ['retention:tombstone', 'retention:cleanup'],
        expect.arrayContaining([`${entityPrefix}${kind}-000`]),
      ]);
      expect(storage.getManyCalls[1]).toHaveLength(100);
    },
  );

  it('caps a direct listSteps request and hydrates it in bounded native batches', async () => {
    const fleet = new FakeFleet({ runs: WorkflowRunDO });
    const run = fleet
      .namespace('runs')
      .get({ toString: () => 'wrun_large_steps' }) as WorkflowRunDO;
    const storage = fleet.cell('runs', 'wrun_large_steps').storage;
    for (let index = 0; index < 1_001; index++) {
      const id = `step-${String(index).padStart(4, '0')}`;
      storage.data.set(creationKey(STEP_CREATED_KEY_PREFIX, index, id), id);
      storage.data.set(`${STEP_KEY_PREFIX}${id}`, { stepId: id });
    }
    storage.resetOperationCounts();

    const result = await run.listSteps({ limit: 10_000 });
    expect(result.ok && result.value.data).toHaveLength(1_000);
    expect(result.ok && result.value.hasMore).toBe(true);

    const entityReads = storage.getManyCalls
      .filter((keys) => keys[0]?.startsWith(STEP_KEY_PREFIX))
      .map((keys) => keys.length);
    expect(entityReads).toEqual([128, 128, 128, 128, 128, 128, 128, 104]);
    expect(entityReads.every((size) => size <= 128)).toBe(true);
  });
});
