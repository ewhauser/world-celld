import { HookNotFoundError, RunExpiredError } from '@workflow/errors';
import { afterEach, describe, expect, it } from 'vitest';
import { createCelldWorld } from '../src/index.js';
import { INDEX_MARKER_PREFIX } from '../src/retention.js';
import { startHarness, type Harness } from '../src/testing/http-harness.js';
import type { IndexDO } from '../src/worker/durable-objects/IndexDO.js';
import type { QueueDO } from '../src/worker/durable-objects/QueueDO.js';

async function createCompletedRun(world: ReturnType<typeof createCelldWorld>, suffix: string) {
  const created = await world.events.create(null, {
    eventType: 'run_created',
    eventData: {
      deploymentId: 'retention-tests',
      workflowName: `retention-${suffix}`,
      input: [`input-${suffix}`],
    },
  });
  const runId = created.run.runId;
  await world.events.create(runId, { eventType: 'run_started' });
  await world.events.create(runId, {
    eventType: 'step_created',
    correlationId: `step-${suffix}`,
    eventData: { stepName: 'retained-step', input: [`step-input-${suffix}`] },
  });
  return runId;
}

async function finishRun(world: ReturnType<typeof createCelldWorld>, runId: string) {
  await world.events.create(runId, {
    eventType: 'run_completed',
    eventData: { output: ['done'] },
  });
}

async function driveCleanup(
  harness: Harness,
  world: ReturnType<typeof createCelldWorld>,
  runId: string,
) {
  for (let attempt = 0; attempt < 20; attempt++) {
    harness.fleet.advance(10);
    await harness.fleet.fireDueAlarms();
    const status = await world.retention.getStatus(runId);
    if (status?.phase === 'tombstoned') return status;
  }
  throw new Error('cleanup did not reach tombstoned state');
}

describe('terminal workflow retention', () => {
  let harness: Harness | undefined;

  afterEach(async () => {
    delete process.env.CELLD_QUEUE_MODE;
    await harness?.close();
    harness = undefined;
  });

  it('purges payloads, indexes, streams, and queued work without allowing resurrection', async () => {
    process.env.CELLD_QUEUE_MODE = 'cells';
    harness = await startHarness({ secret: 'retention-secret', virtualClock: true });
    const world = createCelldWorld({
      fleetUrl: harness.url,
      secret: 'retention-secret',
      deploymentId: 'retention-tests',
      baseUrl: 'http://127.0.0.1:1',
      runRetentionMs: 1_000,
    });

    const runId = await createCompletedRun(world, 'complete');
    await world.writeToStream('retention-stream', runId, 'payload');
    await world.closeStream('retention-stream', runId);
    await world.events.create(runId, {
      eventType: 'hook_created',
      correlationId: 'retention-hook',
      eventData: { token: 'retention-token' },
    });
    await world.queue(
      '__wkf_workflow_retention',
      { runId },
      { delaySeconds: 3_600, idempotencyKey: `wake:${runId}` },
    );
    // Simulate an index and marker written by an older deployment. New
    // correlated events create neither, but retention must still clean old
    // persisted state after an in-place upgrade.
    const legacyCorrelationKey = `correlation:legacy:1:evnt_legacy:${runId}`;
    const index = harness.fleet.namespace('index').get({ toString: () => 'index' }) as IndexDO;
    await index.putOwned(runId, legacyCorrelationKey, JSON.stringify({ runId }));
    await harness.fleet
      .cell('runs', runId)
      .storage.put(
        `${INDEX_MARKER_PREFIX}${encodeURIComponent(legacyCorrelationKey)}`,
        legacyCorrelationKey,
      );
    await finishRun(world, runId);

    const retained = await world.runs.get(runId);
    expect(retained.status).toBe('completed');
    expect(retained.expiredAt).toBeInstanceOf(Date);
    expect((await world.getStreamInfo('retention-stream', runId)).done).toBe(true);

    harness.fleet.advance(1_001);
    const status = await driveCleanup(harness, world, runId);
    expect(status).toMatchObject({
      phase: 'tombstoned',
      deletedStreams: 1,
      deletedQueueMessages: 1,
    });
    expect(status.deletedPayloadKeys).toBeGreaterThan(0);

    await expect(world.runs.get(runId)).rejects.toSatisfy((error) => RunExpiredError.is(error));
    await expect(world.events.create(runId, { eventType: 'run_started' })).rejects.toSatisfy(
      (error) => RunExpiredError.is(error),
    );
    await expect(world.getStreamInfo('retention-stream', runId)).rejects.toThrow(/expired/);
    await expect(world.writeToStream('retention-stream', runId, 'late')).rejects.toThrow(/expired/);
    await expect(
      world.queue('__wkf_workflow_retention', { runId }, { idempotencyKey: `late:${runId}` }),
    ).rejects.toSatisfy((error) => RunExpiredError.is(error));
    await expect(world.hooks.getByToken('retention-token')).rejects.toSatisfy((error) =>
      HookNotFoundError.is(error),
    );
    expect(await index.get(legacyCorrelationKey)).toBeNull();

    const listed = await world.runs.list({ workflowName: 'retention-complete' });
    expect(listed.data).toEqual([]);
    const queue = harness.fleet.namespace('queue').get({ toString: () => 'q:0' }) as QueueDO;
    expect(await queue.stats()).toMatchObject({ pending: 0, inflight: 0, deadLetters: 0 });

    const runKeys = Array.from(harness.fleet.cell('runs', runId).storage.data.keys()).toSorted();
    expect(runKeys).toEqual(['retention:cleanup', 'retention:tombstone']);
    expect(
      Array.from(
        harness.fleet.cell('streams', `run-streams:${runId}`).storage.data.keys(),
      ).toSorted(),
    ).toEqual(['registry:expired', 'registry:owner']);
    expect(
      Array.from(harness.fleet.cell('streams', 'stream:retention-stream').storage.data.keys()),
    ).toEqual(['meta']);
  });

  it('keeps retention disabled by default', async () => {
    harness = await startHarness({ secret: 'retention-secret', virtualClock: true });
    const world = createCelldWorld({
      fleetUrl: harness.url,
      secret: 'retention-secret',
      deploymentId: 'retention-tests',
    });
    const runId = await createCompletedRun(world, 'disabled');
    await finishRun(world, runId);

    harness.fleet.advance(365 * 24 * 60 * 60 * 1_000);
    await harness.fleet.fireDueAlarms();

    expect(await world.retention.getStatus(runId)).toBeNull();
    expect((await world.runs.get(runId)).status).toBe('completed');
    expect(() => world.retention.schedule(runId)).toThrow(/runRetentionMs/);

    await world.retention.cleanupNow(runId);
    const status = await driveCleanup(harness, world, runId);
    expect(status.phase).toBe('tombstoned');
    await expect(world.runs.get(runId)).rejects.toSatisfy((error) => RunExpiredError.is(error));
  });

  it('persists a failed phase and retries it idempotently', async () => {
    harness = await startHarness({ secret: 'retention-secret', virtualClock: true });
    const world = createCelldWorld({
      fleetUrl: harness.url,
      secret: 'retention-secret',
      deploymentId: 'retention-tests',
      runRetentionMs: 100,
    });
    const runId = await createCompletedRun(world, 'retry');
    await finishRun(world, runId);

    const indexSlot = harness.fleet.cell('index', 'index');
    indexSlot.storage.failNextMutation(
      (mutation) => mutation.operation === 'delete' && mutation.key.startsWith('run:'),
      new Error('injected index cleanup failure'),
    );
    harness.fleet.advance(101);
    await harness.fleet.fireDueAlarms();

    expect(await world.retention.getStatus(runId)).toMatchObject({
      phase: 'index',
      attempts: 1,
      lastError: 'injected index cleanup failure',
    });

    harness.fleet.advance(1_000);
    const status = await driveCleanup(harness, world, runId);
    expect(status.phase).toBe('tombstoned');

    const index = harness.fleet.namespace('index').get({ toString: () => 'index' }) as IndexDO;
    const entries = await index.list({ prefix: 'run:retention-retry:' });
    expect(entries.keys).toEqual([]);
  });
});
