import { createCelldWorld as createBaselineWorld } from '@world-celld/baseline';
import { startHarness as startBaselineHarness } from '@world-celld/baseline/testing';
import { afterEach, expect, it } from 'vitest';
import { createCelldWorld } from '../src/index.js';
import { startHarness } from '../src/testing/index.js';

const secret = 'upgrade-fixture-secret';
const deploymentId = 'upgrade-fixture';
const workflowName = 'v090-inflight';
const baseUrl = 'http://127.0.0.1';
const previousQueueMode = process.env.CELLD_QUEUE_MODE;

type CellState = {
  storage: { data: Map<string, unknown>; alarmAt: number | null };
};

function storedCells(fleet: object): Map<string, CellState> {
  return (fleet as { cells: Map<string, CellState> }).cells;
}

const harnesses: Array<{ close(): Promise<void> }> = [];
afterEach(async () => {
  await Promise.all(harnesses.splice(0).map((harness) => harness.close()));
  if (previousQueueMode === undefined) delete process.env.CELLD_QUEUE_MODE;
  else process.env.CELLD_QUEUE_MODE = previousQueueMode;
});

it('resumes a v0.9.0 spec-v8 run, hook, and stream after upgrading the adapter', async () => {
  process.env.CELLD_QUEUE_MODE = 'native';
  const before = await startBaselineHarness({ secret });
  harnesses.push(before);
  const oldWorld = createBaselineWorld({ fleetUrl: before.url, secret, baseUrl, deploymentId });
  const created = await oldWorld.events.create(null, {
    eventType: 'run_created',
    eventData: { deploymentId, workflowName, input: ['old-input'] },
  });
  const runId = created.run.runId;
  expect(created.run.specVersion).toBe(8);
  await oldWorld.events.create(runId, { eventType: 'run_started' });
  await oldWorld.events.create(runId, {
    eventType: 'step_started',
    correlationId: 'old-step',
    eventData: { stepName: 'old-step', input: ['old-input'] },
  });
  await oldWorld.events.create(runId, {
    eventType: 'step_completed',
    correlationId: 'old-step',
    eventData: { result: ['old-output'] },
  });
  await oldWorld.events.create(runId, {
    eventType: 'hook_created',
    correlationId: 'old-hook',
    eventData: { token: 'old-hook-token' },
  });
  await oldWorld.writeToStream('old-stream', runId, 'before-upgrade');
  const queued = await oldWorld.queue(
    '__wkf_workflow_upgrade_fixture',
    { runId },
    { idempotencyKey: 'upgrade-queue-key', delaySeconds: 3600 },
  );
  expect(before.queueMessages).toHaveLength(1);

  const snapshot = Array.from(storedCells(before.fleet), ([key, cell]) => ({
    key,
    data: structuredClone(cell.storage.data),
    alarmAt: cell.storage.alarmAt,
  }));
  await before.close();
  harnesses.pop();

  const after = await startHarness({ secret });
  harnesses.push(after);
  for (const { key, data, alarmAt } of snapshot) {
    const separator = key.indexOf('\0');
    const binding = key.slice(0, separator);
    const name = key.slice(separator + 1);
    const cell = after.fleet.cell(binding, name);
    cell.storage.data = data;
    cell.storage.alarmAt = alarmAt;
    after.fleet.restartCell(binding, name);
  }

  const world = createCelldWorld({ fleetUrl: after.url, secret, baseUrl, deploymentId });
  expect((await world.runs.get(runId)).status).toBe('running');
  expect((await world.steps.get(runId, 'old-step')).status).toBe('completed');
  expect((await world.getStreamChunks('old-stream', runId, {})).data).toHaveLength(1);
  const repeatedQueue = await world.queue(
    '__wkf_workflow_upgrade_fixture',
    { runId },
    { idempotencyKey: 'upgrade-queue-key', delaySeconds: 3600 },
  );
  expect(repeatedQueue.messageId).toBe(queued.messageId);
  expect(after.queueMessages).toHaveLength(0);

  const resumed = await world.events.create(
    runId,
    {
      eventType: 'hook_received',
      correlationId: 'old-hook',
      eventData: { payload: ['new-payload'] },
    },
    { resumeId: 'upgrade-resume', resumePayloadDigest: 'upgrade-digest' },
  );
  const duplicate = await world.events.create(
    runId,
    {
      eventType: 'hook_received',
      correlationId: 'old-hook',
      eventData: { payload: ['new-payload'] },
    },
    { resumeId: 'upgrade-resume', resumePayloadDigest: 'upgrade-digest' },
  );
  expect(duplicate.event?.eventId).toBe(resumed.event?.eventId);
  await world.events.create(runId, { eventType: 'run_completed', eventData: { output: ['done'] } });
  expect((await world.runs.get(runId)).status).toBe('completed');
  expect(
    (await world.events.list({ runId, pagination: { sortOrder: 'asc' } })).data.map(
      (event) => event.eventType,
    ),
  ).toEqual([
    'run_created',
    'run_started',
    'step_created',
    'step_started',
    'step_completed',
    'hook_created',
    'hook_received',
    'run_completed',
  ]);
});
