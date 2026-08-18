import { HookNotFoundError, RunExpiredError } from '@workflow/errors';
import type { Hook } from '@workflow/world';
import { SPEC_VERSION_CURRENT } from '@workflow/world';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { createCelldWorld } from '../src/index.js';
import { CLEANUP_RECORD_KEY, type CleanupRecord } from '../src/retention.js';
import { FakeFleet } from '../src/testing/fake-cell.js';
import { startHarness, type Harness } from '../src/testing/http-harness.js';
import { stringify } from '../src/vendor/shared/index.js';
import type { IndexDO } from '../src/worker/durable-objects/IndexDO.js';
import type { QueueDO } from '../src/worker/durable-objects/QueueDO.js';
import type { StreamDO } from '../src/worker/durable-objects/StreamDO.js';
import { WorkflowRunDO } from '../src/worker/durable-objects/WorkflowRunDO.js';

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

async function driveTerminalCleanup(harness: Harness, runId: string) {
  for (let page = 0; page < 20; page++) {
    if (!harness.fleet.cell('runs', runId).storage.data.has('terminal:cleanup')) return;
    harness.fleet.advance(1);
    await harness.fleet.fireDueAlarms();
  }
  throw new Error('terminal cleanup did not complete');
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
    await driveTerminalCleanup(harness, runId);

    const indexSlot = harness.fleet.cell('index', 'index');
    indexSlot.storage.failNextMutation(
      (mutation) => mutation.operation === 'delete' && mutation.key.startsWith('run:'),
      new Error('injected index cleanup failure'),
    );
    harness.fleet.advance(101);
    await harness.fleet.fireDueAlarms();
    harness.fleet.advance(1);
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

  it('does at most one bounded cleanup page per alarm and resumes a large stream', async () => {
    harness = await startHarness({ secret: 'retention-secret', virtualClock: true });
    const world = createCelldWorld({
      fleetUrl: harness.url,
      secret: 'retention-secret',
      deploymentId: 'retention-tests',
      runRetentionMs: 100,
    });
    const runId = await createCompletedRun(world, 'paged-stream');
    const streamName = 'retention-paged-stream';
    const registry = harness.fleet.namespace('streams').get({
      toString: () => `run-streams:${runId}`,
    }) as StreamDO;
    const stream = harness.fleet.namespace('streams').get({
      toString: () => `stream:${streamName}`,
    }) as StreamDO;
    await registry.registerStream(runId, streamName);
    for (let offset = 0; offset < 300; offset += 32) {
      await stream.writeChunks(
        runId,
        Array.from({ length: Math.min(32, 300 - offset) }, () => new Uint8Array(1024)),
      );
    }
    await finishRun(world, runId);
    await driveTerminalCleanup(harness, runId);

    harness.fleet.advance(101);
    await harness.fleet.fireDueAlarms();
    expect(await world.retention.getStatus(runId)).toMatchObject({ phase: 'index' });
    const streamStorage = harness.fleet.cell('streams', `stream:${streamName}`).storage;
    const remainingChunks = () =>
      Array.from(streamStorage.data.keys()).filter((key) => key.startsWith('chunk:')).length;
    expect(remainingChunks()).toBe(300);

    harness.fleet.advance(1);
    await harness.fleet.fireDueAlarms();
    expect(await world.retention.getStatus(runId)).toMatchObject({ phase: 'streams' });
    expect(remainingChunks()).toBe(300);

    harness.fleet.advance(1);
    await harness.fleet.fireDueAlarms();
    expect(await world.retention.getStatus(runId)).toMatchObject({
      phase: 'streams',
      deletedStreams: 0,
    });
    expect(remainingChunks()).toBe(236);

    const status = await driveCleanup(harness, world, runId);
    expect(status).toMatchObject({ phase: 'tombstoned', deletedStreams: 1 });
    expect(remainingChunks()).toBe(0);
  });

  it('pages terminal hooks and waits with bounded operations until cleanup completes', async () => {
    harness = await startHarness({ secret: 'retention-secret', virtualClock: true });
    const world = createCelldWorld({
      fleetUrl: harness.url,
      secret: 'retention-secret',
      deploymentId: 'retention-tests',
    });
    const runId = await createCompletedRun(world, 'terminal-pages');
    const runStorage = harness.fleet.cell('runs', runId).storage;
    const indexStorage = harness.fleet.cell('index', 'index').storage;

    for (let hookIndex = 0; hookIndex < 150; hookIndex++) {
      const hookId = `hook-${String(hookIndex).padStart(3, '0')}`;
      const token = `token-${String(hookIndex).padStart(3, '0')}`;
      const createdAt = new Date(harness.fleet.now + hookIndex);
      const hook = {
        runId,
        hookId,
        token,
        ownerId: '',
        projectId: '',
        environment: '',
        createdAt,
        specVersion: SPEC_VERSION_CURRENT,
        isWebhook: false,
      } as Hook;
      runStorage.data.set(`hook:${hookId}`, hook);
      runStorage.data.set(`hookcreated:${createdAt.toISOString()}:${hookId}`, hookId);
      const serialized = stringify(hook);
      indexStorage.data.set(`hook:${token}`, serialized);
      indexStorage.data.set(`hookid:${hookId}`, serialized);
    }
    for (let waitIndex = 0; waitIndex < 300; waitIndex++) {
      runStorage.data.set(`wait:wait-${String(waitIndex).padStart(3, '0')}`, { waitIndex });
    }

    await finishRun(world, runId);
    expect(runStorage.data.has('terminal:cleanup')).toBe(true);
    await expect(world.hooks.getByToken('token-000')).rejects.toSatisfy((error) =>
      HookNotFoundError.is(error),
    );

    const hookCounts: number[] = [];
    const waitCounts: number[] = [];
    for (let page = 0; page < 10 && runStorage.data.has('terminal:cleanup'); page++) {
      runStorage.operationCalls.length = 0;
      runStorage.listCalls.length = 0;
      harness.fleet.advance(1);
      await harness.fleet.fireDueAlarms();

      expect(
        runStorage.operationCalls
          .filter((call) => call.operation === 'delete')
          .every((call) => call.keys.length <= 128),
      ).toBe(true);
      expect(runStorage.listCalls.every((call) => (call.options.limit ?? 0) <= 129)).toBe(true);
      hookCounts.push(
        Array.from(runStorage.data.keys()).filter((key) => key.startsWith('hook:')).length,
      );
      waitCounts.push(
        Array.from(runStorage.data.keys()).filter((key) => key.startsWith('wait:')).length,
      );
    }

    expect(hookCounts).toEqual([86, 22, 0, 0, 0, 0]);
    expect(waitCounts).toEqual([300, 300, 300, 172, 44, 0]);
    expect(runStorage.data.has('terminal:cleanup')).toBe(false);
    expect(runStorage.alarmAt).toBeNull();
    expect(
      Array.from(indexStorage.data.keys()).filter(
        (key) => key.startsWith('hook:') || key.startsWith('hookid:'),
      ),
    ).toEqual([]);
    expect(indexStorage.data.has(`terminal:${runId}`)).toBe(true);
  });

  it('retries a failed terminal page without losing its local cursor', async () => {
    harness = await startHarness({ secret: 'retention-secret', virtualClock: true });
    const world = createCelldWorld({
      fleetUrl: harness.url,
      secret: 'retention-secret',
      deploymentId: 'retention-tests',
    });
    const runId = await createCompletedRun(world, 'terminal-retry');
    const runStorage = harness.fleet.cell('runs', runId).storage;
    const indexStorage = harness.fleet.cell('index', 'index').storage;
    const hookId = 'hook-retry';
    const token = 'token-retry';
    const hook = {
      runId,
      hookId,
      token,
      ownerId: '',
      projectId: '',
      environment: '',
      createdAt: new Date(harness.fleet.now),
      specVersion: SPEC_VERSION_CURRENT,
      isWebhook: false,
    } as Hook;
    runStorage.data.set(`hook:${hookId}`, hook);
    runStorage.data.set(`hookcreated:${hook.createdAt.toISOString()}:${hookId}`, hookId);
    indexStorage.data.set(`hook:${token}`, stringify(hook));
    indexStorage.data.set(`hookid:${hookId}`, stringify(hook));
    await finishRun(world, runId);

    indexStorage.failNextMutation(
      (mutation) => mutation.operation === 'delete' && mutation.key === `hook:${token}`,
      new Error('injected terminal cleanup crash'),
    );
    harness.fleet.advance(1);
    await harness.fleet.fireDueAlarms();

    expect(runStorage.data.get('terminal:cleanup')).toMatchObject({
      phase: 'hooks',
      attempts: 1,
      lastError: 'injected terminal cleanup crash',
    });
    expect(runStorage.data.has(`hook:${hookId}`)).toBe(true);
    expect(indexStorage.data.has(`hook:${token}`)).toBe(true);

    harness.fleet.advance(1_000);
    await harness.fleet.fireDueAlarms();
    harness.fleet.advance(1);
    await harness.fleet.fireDueAlarms();
    expect(runStorage.data.has('terminal:cleanup')).toBe(false);
    expect(runStorage.data.has(`hook:${hookId}`)).toBe(false);
    expect(indexStorage.data.has(`hook:${token}`)).toBe(false);
    expect(indexStorage.data.has(`hookid:${hookId}`)).toBe(false);
  });

  it('does not regress queue cleanup progress when concurrent pages resolve out of order', async () => {
    let fleet!: FakeFleet;
    let firstStarted!: () => void;
    let releaseFirst!: () => void;
    const firstStartedPromise = new Promise<void>((resolve) => {
      firstStarted = resolve;
    });
    const firstReleasePromise = new Promise<void>((resolve) => {
      releaseFirst = resolve;
    });
    let calls = 0;
    const expireRun = vi.fn<() => Promise<{ deleted: number; done: boolean }>>(async () => {
      calls++;
      if (calls === 1) {
        firstStarted();
        await firstReleasePromise;
        return { deleted: 64, done: false };
      }
      return { deleted: 128, done: false };
    });
    const queueNamespace = {
      idFromName: (name: string) => ({ toString: () => name }),
      get: () => ({ expireRun }),
    };
    fleet = new FakeFleet(
      { runs: WorkflowRunDO },
      { clock: () => fleet.now, WORKFLOW_QUEUE: queueNamespace },
    );
    const runId = 'wrun_concurrent_queue_cleanup';
    const run = fleet.namespace('runs').get({ toString: () => runId }) as WorkflowRunDO;
    const storage = fleet.cell('runs', runId).storage;
    storage.data.set(CLEANUP_RECORD_KEY, {
      version: 1,
      runId,
      workflowName: 'concurrent-cleanup',
      createdAt: new Date(fleet.now - 2_000),
      completedAt: new Date(fleet.now - 1_000),
      terminalStatus: 'completed',
      dueAt: new Date(fleet.now),
      queueShards: 1,
      phase: 'queues',
      generation: 0,
      attempts: 0,
      deletedPayloadKeys: 0,
      deletedStreams: 0,
      deletedQueueMessages: 0,
    } satisfies CleanupRecord);

    const first = run.cleanupNow({ retentionMs: 1, queueShards: 1 });
    await firstStartedPromise;
    const second = run.cleanupNow({ retentionMs: 1, queueShards: 1 });
    await vi.waitFor(() => expect(expireRun).toHaveBeenCalledTimes(2));
    await second;
    releaseFirst();
    await first;

    expect(storage.data.get(CLEANUP_RECORD_KEY)).toMatchObject({
      phase: 'queues',
      deletedQueueMessages: 128,
    });
    expect(storage.data.get('retention:progress')).toEqual({
      queueShard: 0,
      queueShardDeleted: 128,
    });
  });

  it('ignores a late cleanup failure after a concurrent page advances the generation', async () => {
    let fleet!: FakeFleet;
    let firstStarted!: () => void;
    let rejectFirst!: () => void;
    const firstStartedPromise = new Promise<void>((resolve) => {
      firstStarted = resolve;
    });
    const firstFailurePromise = new Promise<void>((resolve) => {
      rejectFirst = resolve;
    });
    let calls = 0;
    const expireRun = vi.fn<() => Promise<{ deleted: number; done: boolean }>>(async () => {
      calls++;
      if (calls === 1) {
        firstStarted();
        await firstFailurePromise;
        throw new Error('late queue failure');
      }
      return { deleted: 128, done: false };
    });
    fleet = new FakeFleet(
      { runs: WorkflowRunDO },
      {
        clock: () => fleet.now,
        WORKFLOW_QUEUE: {
          idFromName: (name: string) => ({ toString: () => name }),
          get: () => ({ expireRun }),
        },
      },
    );
    const runId = 'wrun_late_queue_failure';
    const run = fleet.namespace('runs').get({ toString: () => runId }) as WorkflowRunDO;
    const storage = fleet.cell('runs', runId).storage;
    storage.data.set(CLEANUP_RECORD_KEY, {
      version: 1,
      runId,
      workflowName: 'late-failure',
      createdAt: new Date(fleet.now - 2_000),
      completedAt: new Date(fleet.now - 1_000),
      terminalStatus: 'completed',
      dueAt: new Date(fleet.now),
      queueShards: 1,
      phase: 'queues',
      generation: 0,
      attempts: 0,
      deletedPayloadKeys: 0,
      deletedStreams: 0,
      deletedQueueMessages: 0,
    } satisfies CleanupRecord);

    const first = run.cleanupNow({ retentionMs: 1, queueShards: 1 });
    await firstStartedPromise;
    const second = run.cleanupNow({ retentionMs: 1, queueShards: 1 });
    await vi.waitFor(() => expect(expireRun).toHaveBeenCalledTimes(2));
    await second;
    rejectFirst();
    await first;

    expect(storage.data.get(CLEANUP_RECORD_KEY)).toMatchObject({
      phase: 'queues',
      deletedQueueMessages: 128,
      attempts: 0,
    });
    expect(storage.data.get(CLEANUP_RECORD_KEY)).toMatchObject({ lastError: undefined });
  });
});
