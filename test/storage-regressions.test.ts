import { setTimeout } from 'node:timers/promises';
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
    return result.run!;
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
    const put = index.put.bind(index);
    let failNextRunIndexWrite = true;
    index.put = async (key, value) => {
      if (failNextRunIndexWrite && key.startsWith('run:')) {
        failNextRunIndexWrite = false;
        throw new Error('injected index outage');
      }
      await put(key, value);
    };

    try {
      await expect(storage.events.create(runId, event)).rejects.toThrow('injected index outage');
    } finally {
      index.put = put;
    }

    expect((await storage.runs.get(runId)).status).toBe('running');

    // Replaying the durable event must also repair any missing secondary index.
    await storage.events.create(runId, event);
    const listed = await storage.runs.list({ workflowName: 'index-reconciliation' });
    expect(listed.data.map((run) => run.runId)).toContain(runId);
  });

  it('reconciles both hook indexes after a partial post-commit write', async () => {
    const run = await createRun('hook-index-reconciliation');
    const hookEvent = {
      eventType: 'hook_created' as const,
      correlationId: 'hook-partial-index',
      eventData: { token: 'partial-index-token' },
    };
    const index = mockEnv.WORKFLOW_INDEX;
    const put = index.put.bind(index);
    let failHookIdWrite = true;
    index.put = async (key, value) => {
      if (failHookIdWrite && key === 'hookid:hook-partial-index') {
        failHookIdWrite = false;
        throw new Error('injected hook-id index outage');
      }
      await put(key, value);
    };

    try {
      await expect(storage.events.create(run.runId, hookEvent)).rejects.toThrow(
        'injected hook-id index outage',
      );
    } finally {
      index.put = put;
    }

    // The hook and token index committed, so replay is the recovery opportunity.
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
    await storage.events.create(run.runId, {
      eventType: 'step_created',
      correlationId: 'correlation-123',
      eventData: { stepName: 'correlated-step', input: [] },
    });

    const result = await storage.events.listByCorrelationId({
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

    expect(result.data.map((run) => run.runId)).toEqual([
      third.runId,
      second.runId,
      first.runId,
    ]);
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
