import { EntityConflictError, RunNotSupportedError } from '@workflow/errors';
import { getMaxEventsPerRun, SPEC_VERSION_CURRENT } from '@workflow/world';
import { beforeEach, describe, expect, it } from 'vitest';
import { createStorage } from '../src/storage.js';
import { clearMockData, createMockEnv } from '../src/test-mocks.js';

describe('Workflow 5 contract regressions', () => {
  let storage: ReturnType<typeof createStorage>;

  beforeEach(() => {
    clearMockData();
    storage = createStorage({
      env: createMockEnv(),
      deploymentId: 'workflow-5-regressions',
    });
  });

  async function createRun(workflowName: string) {
    const result = await storage.events.create(null, {
      eventType: 'run_created',
      eventData: {
        deploymentId: 'workflow-5-regressions',
        workflowName,
        input: [],
      },
    });
    return result.run;
  }

  async function listEvents(runId: string) {
    return storage.events.list({
      runId,
      pagination: { sortOrder: 'asc' },
    });
  }

  it('converges repeated hook resumes on one event and validates the payload digest', async () => {
    const run = await createRun('resume-idempotency');
    await storage.events.create(run.runId, {
      eventType: 'hook_created',
      correlationId: 'hook-a',
      eventData: { token: 'resume-token' },
    });
    const received = {
      eventType: 'hook_received' as const,
      correlationId: 'hook-a',
      eventData: { payload: ['resumed'] },
    };
    const params = {
      resumeId: 'resume-a',
      resumePayloadDigest: 'digest-a',
    };

    const first = await storage.events.create(run.runId, received, params);
    const replay = await storage.events.create(run.runId, received, params);

    expect(replay.event?.eventId).toBe(first.event?.eventId);
    expect(
      (await listEvents(run.runId)).data.filter((event) => event.eventType === 'hook_received'),
    ).toHaveLength(1);

    await expect(
      storage.events.create(run.runId, received, {
        ...params,
        resumePayloadDigest: 'different-digest',
      }),
    ).rejects.toSatisfy((error) => EntityConflictError.is(error));

    await storage.events.create(run.runId, {
      eventType: 'hook_created',
      correlationId: 'hook-b',
      eventData: { token: 'other-resume-token' },
    });
    await expect(
      storage.events.create(run.runId, { ...received, correlationId: 'hook-b' }, params),
    ).rejects.toSatisfy((error) => EntityConflictError.is(error));

    await storage.events.create(run.runId, {
      eventType: 'hook_disposed',
      correlationId: 'hook-a',
    });
    const replayAfterDisposal = await storage.events.create(run.runId, received, params);
    expect(replayAfterDisposal.event?.eventId).toBe(first.event?.eventId);
  });

  it('rejects child creation and attribute mutation after a run is terminal', async () => {
    const run = await createRun('terminal-guards');
    await storage.events.create(run.runId, { eventType: 'run_started' });
    await storage.events.create(run.runId, {
      eventType: 'run_completed',
      eventData: { output: [] },
    });

    await expect(
      storage.events.create(run.runId, {
        eventType: 'step_started',
        correlationId: 'late-step',
        eventData: { stepName: 'late-step', input: [] },
      }),
    ).rejects.toSatisfy((error) => EntityConflictError.is(error));
    await expect(
      storage.events.create(run.runId, {
        eventType: 'attr_set',
        correlationId: 'late-attributes',
        eventData: {
          changes: [{ key: 'late', value: 'true' }],
          writer: { type: 'workflow' },
        },
      }),
    ).rejects.toSatisfy((error) => EntityConflictError.is(error));

    expect((await listEvents(run.runId)).data).toHaveLength(3);
  });

  it('persists wait state and rejects duplicate creation or completion', async () => {
    const run = await createRun('wait-lifecycle');
    const resumeAt = new Date('2026-08-17T00:00:00.000Z');
    const created = await storage.events.create(run.runId, {
      eventType: 'wait_created',
      correlationId: 'wait-a',
      eventData: { resumeAt },
    });

    expect(created.wait).toMatchObject({
      waitId: `${run.runId}-wait-a`,
      status: 'waiting',
      resumeAt,
    });
    await expect(
      storage.events.create(run.runId, {
        eventType: 'wait_created',
        correlationId: 'wait-a',
        eventData: { resumeAt },
      }),
    ).rejects.toSatisfy((error) => EntityConflictError.is(error));

    const completed = await storage.events.create(run.runId, {
      eventType: 'wait_completed',
      correlationId: 'wait-a',
    });
    expect(completed.wait).toMatchObject({
      waitId: `${run.runId}-wait-a`,
      status: 'completed',
      createdAt: created.wait?.createdAt,
    });
    await expect(
      storage.events.create(run.runId, {
        eventType: 'wait_completed',
        correlationId: 'wait-a',
      }),
    ).rejects.toSatisfy((error) => EntityConflictError.is(error));
  });

  it('returns maxEvents and the preload for an idempotent run_started', async () => {
    const run = await createRun('start-replay');
    await storage.events.create(run.runId, { eventType: 'run_started' });

    const replay = await storage.events.create(run.runId, { eventType: 'run_started' });

    expect(replay.event).toBeUndefined();
    expect(replay.maxEvents).toBe(getMaxEventsPerRun());
    expect(replay.events?.map((event) => event.eventType)).toEqual(['run_created', 'run_started']);
    expect(replay.cursor).toBeNull();
    expect(replay.hasMore).toBe(false);

    const withoutPreload = await storage.events.create(
      run.runId,
      { eventType: 'run_started' },
      { skipPreload: true },
    );
    expect(withoutPreload.maxEvents).toBe(getMaxEventsPerRun());
    expect(withoutPreload.events).toBeUndefined();
  });

  it('advertises maxEvents on every response that carries a run', async () => {
    const created = await storage.events.create(null, {
      eventType: 'run_created',
      eventData: {
        deploymentId: 'workflow-5-regressions',
        workflowName: 'run-event-ceiling',
        input: [],
      },
    });
    expect(created.maxEvents).toBe(getMaxEventsPerRun());

    const completed = await storage.events.create(created.run.runId, {
      eventType: 'run_completed',
      eventData: { output: [] },
    });
    expect(completed.maxEvents).toBe(getMaxEventsPerRun());
  });

  it('allows only the lazy step creator to start the step', async () => {
    const run = await createRun('lazy-step-idempotency');
    const lazyStart = {
      eventType: 'step_started' as const,
      correlationId: 'lazy-step-once',
      eventData: { stepName: 'lazy-step-once', input: ['input'] },
    };

    const first = await storage.events.create(run.runId, lazyStart);
    expect(first.stepCreated).toBe(true);
    await expect(storage.events.create(run.runId, lazyStart)).rejects.toSatisfy((error) =>
      EntityConflictError.is(error),
    );

    const events = await listEvents(run.runId);
    expect(events.data.map((event) => event.eventType)).toEqual([
      'run_created',
      'step_created',
      'step_started',
    ]);
  });

  it('deduplicates workflow-authored attribute events by correlation id', async () => {
    const run = await createRun('attribute-idempotency');
    const attrSet = {
      eventType: 'attr_set' as const,
      correlationId: 'attribute-write-a',
      eventData: {
        changes: [{ key: 'region', value: 'us-west' }],
        writer: { type: 'workflow' as const },
      },
    };

    await storage.events.create(run.runId, attrSet);
    await expect(storage.events.create(run.runId, attrSet)).rejects.toSatisfy((error) =>
      EntityConflictError.is(error),
    );

    const events = await listEvents(run.runId);
    expect(events.data.filter((event) => event.eventType === 'attr_set')).toHaveLength(1);
    await expect(storage.runs.get(run.runId)).resolves.toMatchObject({
      attributes: { region: 'us-west' },
    });
  });

  it('stores serialized inputs only on their canonical creation events', async () => {
    const runId = 'wrun_canonical_payloads';
    await storage.events.create(runId, {
      eventType: 'run_started',
      eventData: {
        deploymentId: 'workflow-5-regressions',
        workflowName: 'canonical-payloads',
        input: ['run-input'],
      },
    });
    await storage.events.create(runId, {
      eventType: 'step_started',
      correlationId: 'lazy-step',
      eventData: {
        stepName: 'lazy-step',
        input: ['step-input'],
      },
    });

    const events = (await listEvents(runId)).data;
    expect(events.find((event) => event.eventType === 'run_created')).toHaveProperty(
      'eventData.input',
      ['run-input'],
    );
    expect(events.find((event) => event.eventType === 'run_started')).not.toHaveProperty(
      'eventData',
    );
    expect(events.find((event) => event.eventType === 'step_created')).toHaveProperty(
      'eventData.input',
      ['step-input'],
    );
    expect(events.find((event) => event.eventType === 'step_started')).toMatchObject({
      eventData: { stepName: 'lazy-step' },
    });
    expect(events.find((event) => event.eventType === 'step_started')).not.toHaveProperty(
      'eventData.input',
    );
  });

  it('rejects runs stamped below the current Workflow 5 protocol', async () => {
    await expect(
      storage.events.create(null, {
        eventType: 'run_created',
        specVersion: SPEC_VERSION_CURRENT - 1,
        eventData: {
          deploymentId: 'workflow-5-regressions',
          workflowName: 'unsupported-old-run',
          input: [],
        },
      }),
    ).rejects.toSatisfy((error) => RunNotSupportedError.is(error));
  });
});
