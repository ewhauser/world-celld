import { setTimeout } from 'node:timers/promises';
import { eventIdToSlot, slotToEventId } from '@workflow/world';
import { beforeEach, describe, expect, it } from 'vitest';
import { createStorage } from '../src/storage.js';
import { clearMockData, createMockEnv } from '../src/test-mocks.js';

describe('Storage regressions', () => {
  let mockEnv: ReturnType<typeof createMockEnv>;
  let storage: ReturnType<typeof createStorage>;

  beforeEach(() => {
    clearMockData();
    mockEnv = createMockEnv();
    storage = createStorage({
      env: mockEnv,
      deploymentId: 'regression-tests',
    });
  });

  async function createRun(workflowName = 'regression-workflow') {
    const result = await storage.events.create(null, {
      eventType: 'run_created',
      eventData: {
        deploymentId: 'regression-tests',
        workflowName,
        input: [],
      },
    });
    return result.run;
  }

  it('admits only one hook when two runs concurrently claim the same token', async () => {
    const firstRun = await createRun('hook-race');
    const secondRun = await createRun('hook-race');

    const outcomes = await Promise.all([
      storage.events.create(firstRun.runId, {
        eventType: 'hook_created',
        correlationId: 'hook-a',
        eventData: { token: 'shared-token' },
      }),
      storage.events.create(secondRun.runId, {
        eventType: 'hook_created',
        correlationId: 'hook-b',
        eventData: { token: 'shared-token' },
      }),
    ]);

    expect(outcomes.filter((outcome) => outcome.event?.eventType === 'hook_created')).toHaveLength(
      1,
    );
    expect(outcomes.filter((outcome) => outcome.event?.eventType === 'hook_conflict')).toHaveLength(
      1,
    );

    const [firstHooks, secondHooks] = await Promise.all([
      storage.hooks.list({ runId: firstRun.runId }),
      storage.hooks.list({ runId: secondRun.runId }),
    ]);
    expect([...firstHooks.data, ...secondHooks.data]).toHaveLength(1);
  });

  it('reconciles a committed run when its first index write fails', async () => {
    const runId = 'wrun_index_reconciliation';
    const event = {
      eventType: 'run_started' as const,
      eventData: {
        deploymentId: 'regression-tests',
        workflowName: 'index-reconciliation',
        input: [],
      },
    };
    const index = mockEnv.WORKFLOW_INDEX;
    const putOwned = index.putOwned.bind(index);
    let failNextRunIndexWrite = true;
    index.putOwned = async (ownerRunId, key, value) => {
      if (failNextRunIndexWrite && key.startsWith('run:')) {
        failNextRunIndexWrite = false;
        throw new Error('injected index outage');
      }
      return putOwned(ownerRunId, key, value);
    };

    try {
      await expect(storage.events.create(runId, event)).rejects.toThrow('injected index outage');
    } finally {
      index.putOwned = putOwned;
    }

    expect((await storage.runs.get(runId)).status).toBe('running');

    // Replaying the durable event must also repair any missing secondary index.
    await storage.events.create(runId, event);
    const listed = await storage.runs.list({ workflowName: 'index-reconciliation' });
    expect(listed.data.map((run) => run.runId)).toContain(runId);
  });

  it('reconciles both hook indexes after a post-commit index failure', async () => {
    const run = await createRun('hook-index-reconciliation');
    const hookEvent = {
      eventType: 'hook_created' as const,
      correlationId: 'hook-partial-index',
      eventData: { token: 'partial-index-token' },
    };
    const index = mockEnv.WORKFLOW_INDEX;
    const finalize = index.finalizeHookIndexes.bind(index);
    let failFinalize = true;
    index.finalizeHookIndexes = async (...args) => {
      if (failFinalize) {
        failFinalize = false;
        throw new Error('injected hook index outage');
      }
      await finalize(...args);
    };

    try {
      await expect(storage.events.create(run.runId, hookEvent)).rejects.toThrow(
        'injected hook index outage',
      );
    } finally {
      index.finalizeHookIndexes = finalize;
    }

    // The hook entity committed, so replay is the index recovery opportunity.
    await expect(storage.events.create(run.runId, hookEvent)).resolves.toMatchObject({
      hook: { hookId: 'hook-partial-index', token: 'partial-index-token' },
    });
    await expect(storage.hooks.getByToken('partial-index-token')).resolves.toMatchObject({
      hookId: 'hook-partial-index',
    });
    await expect(storage.hooks.get('hook-partial-index')).resolves.toMatchObject({
      token: 'partial-index-token',
    });
  });

  it('returns events that match listByCorrelationId', async () => {
    const run = await createRun('correlation-index');
    const otherRun = await createRun('correlation-index');
    await storage.events.create(run.runId, {
      eventType: 'step_created',
      correlationId: 'correlation-123',
      eventData: { stepName: 'correlated-step', input: [] },
    });
    await storage.events.create(otherRun.runId, {
      eventType: 'step_created',
      correlationId: 'correlation-123',
      eventData: { stepName: 'other-run-step', input: [] },
    });

    const result = await storage.events.listByCorrelationId({
      runId: run.runId,
      correlationId: 'correlation-123',
      pagination: { sortOrder: 'asc' },
    });

    expect(result.data).toHaveLength(1);
    expect(result.data[0]).toMatchObject({
      runId: run.runId,
      correlationId: 'correlation-123',
      eventType: 'step_created',
    });
  });

  it('uses dense Workflow 5 event slots and reports writes skipped by a stale replay', async () => {
    const run = await createRun('slot-reporting');
    await storage.events.create(run.runId, {
      eventType: 'step_created',
      correlationId: 'step-a',
      eventData: { stepName: 'step-a', input: [] },
    });

    const result = await storage.events.create(
      run.runId,
      {
        eventType: 'step_created',
        correlationId: 'step-b',
        eventData: { stepName: 'step-b', input: [] },
      },
      { eventCount: 1 },
    );

    expect(result.event?.eventId).toBe(slotToEventId(3));
    expect(result.events?.map((event) => event.eventId)).toEqual([slotToEventId(2)]);
    expect(result.cursor).toBeNull();
    expect(result.hasMore).toBe(false);

    const page = await storage.events.list({
      runId: run.runId,
      pagination: { sortOrder: 'asc' },
    });
    expect(page.data.map((event) => eventIdToSlot(event.eventId))).toEqual([1, 2, 3]);
  });

  it('orders runs by creation time in descending order', async () => {
    const first = await createRun('run-sort-order');
    await setTimeout(2);
    const second = await createRun('run-sort-order');
    await setTimeout(2);
    const third = await createRun('run-sort-order');

    const result = await storage.runs.list({
      workflowName: 'run-sort-order',
      pagination: { sortOrder: 'desc' },
    });

    expect(result.data.map((run) => run.runId)).toEqual([third.runId, second.runId, first.runId]);
  });

  it('fills a status-filtered run page before advancing its cursor', async () => {
    await createRun('run-status-pagination');
    await createRun('run-status-pagination');
    const completed = await createRun('run-status-pagination');
    await storage.events.create(completed.runId, {
      eventType: 'run_completed',
      eventData: { output: [] },
    });

    const result = await storage.runs.list({
      workflowName: 'run-status-pagination',
      status: 'completed',
      pagination: { limit: 2, sortOrder: 'asc' },
    });

    expect(result.data.map((run) => run.runId)).toEqual([completed.runId]);
    expect(result.hasMore).toBe(false);
    expect(result.cursor).toBeNull();
  });

  it('does not let earlier non-terminal index metadata hide a later run status', async () => {
    const completed = await createRun('stale-status-index');
    await storage.events.create(completed.runId, {
      eventType: 'run_completed',
      eventData: { output: [] },
    });

    const entries = await mockEnv.WORKFLOW_INDEX.list({ prefix: 'run:stale-status-index:' });
    const entry = entries.keys[0];
    const metadata = JSON.parse(entry.value);
    await mockEnv.WORKFLOW_INDEX.put(
      entry.name,
      JSON.stringify({ ...metadata, status: 'pending' }),
    );

    const result = await storage.runs.list({
      workflowName: 'stale-status-index',
      status: 'completed',
    });

    expect(result.data.map((run) => run.runId)).toEqual([completed.runId]);
  });

  it('orders steps by creation time instead of step id', async () => {
    const run = await createRun('step-sort-order');
    await storage.events.create(run.runId, {
      eventType: 'step_created',
      correlationId: 'z-first-step',
      eventData: { stepName: 'first', input: [] },
    });
    await setTimeout(2);
    await storage.events.create(run.runId, {
      eventType: 'step_created',
      correlationId: 'z-second-step',
      eventData: { stepName: 'second', input: [] },
    });
    await setTimeout(2);
    await storage.events.create(run.runId, {
      eventType: 'step_created',
      correlationId: 'a-third-step',
      eventData: { stepName: 'third', input: [] },
    });

    const ascending = await storage.steps.list({
      runId: run.runId,
      pagination: { sortOrder: 'asc' },
    });
    const descending = await storage.steps.list({
      runId: run.runId,
      pagination: { sortOrder: 'desc' },
    });

    expect(ascending.data.map((step) => step.stepId)).toEqual([
      'z-first-step',
      'z-second-step',
      'a-third-step',
    ]);
    expect(descending.data.map((step) => step.stepId)).toEqual([
      'a-third-step',
      'z-second-step',
      'z-first-step',
    ]);
  });

  it('orders hooks by creation time instead of hook id', async () => {
    const run = await createRun('hook-sort-order');
    await storage.events.create(run.runId, {
      eventType: 'hook_created',
      correlationId: 'z-first-hook',
      eventData: { token: 'first-hook-token' },
    });
    await setTimeout(2);
    await storage.events.create(run.runId, {
      eventType: 'hook_created',
      correlationId: 'z-second-hook',
      eventData: { token: 'second-hook-token' },
    });
    await setTimeout(2);
    await storage.events.create(run.runId, {
      eventType: 'hook_created',
      correlationId: 'a-third-hook',
      eventData: { token: 'third-hook-token' },
    });

    const ascending = await storage.hooks.list({
      runId: run.runId,
      pagination: { sortOrder: 'asc' },
    });
    const descending = await storage.hooks.list({
      runId: run.runId,
      pagination: { sortOrder: 'desc' },
    });

    expect(ascending.data.map((hook) => hook.hookId)).toEqual([
      'z-first-hook',
      'z-second-hook',
      'a-third-hook',
    ]);
    expect(descending.data.map((hook) => hook.hookId)).toEqual([
      'a-third-hook',
      'z-second-hook',
      'z-first-hook',
    ]);
  });
});
