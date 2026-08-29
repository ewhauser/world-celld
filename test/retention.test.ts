import { HookNotFoundError, RunExpiredError } from '@workflow/errors';
import type { Hook } from '@workflow/world';
import { SPEC_VERSION_CURRENT } from '@workflow/world';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { createCelldWorld } from '../src/index.js';
import {
  createWorkflowIndex,
  hookIdShardName,
  hookTokenShardName,
  runCatalogShardName,
} from '../src/indexes.js';
import { CLEANUP_RECORD_KEY, type CleanupRecord } from '../src/retention.js';
import { queueOrphanName, queuePayloadRegistryKey } from '../src/queue-protocol.js';
import { FakeFleet } from '../src/testing/fake-cell.js';
import { startHarness, type Harness } from '../src/testing/http-harness.js';
import { stringify } from '../src/vendor/shared/index.js';
import type { StreamDO } from '../src/worker/durable-objects/StreamDO.js';
import { WorkflowRunDO } from '../src/worker/durable-objects/WorkflowRunDO.js';

function workflowIndex(harness: Harness) {
  return createWorkflowIndex({
    runCatalog: harness.fleet.namespace('run-catalog'),
    hookTokens: harness.fleet.namespace('hook-tokens'),
    hookIds: harness.fleet.namespace('hook-ids'),
  });
}

async function seedHookIndex(harness: Harness, hook: Hook): Promise<void> {
  const owner = { runId: hook.runId, hookId: hook.hookId };
  const admission = await workflowIndex(harness).reserveHook(hook.token, owner);
  if (!admission.admitted) throw new Error('expected seeded hook admission');
  await workflowIndex(harness).finalizeHookIndexes(
    hook.token,
    hook.hookId,
    stringify(hook),
    owner,
    admission.reservation,
  );
}

function hookTokenRecordKey(token: string): string {
  return `hook:${encodeURIComponent(token)}`;
}

function hookIdRecordKey(hookId: string): string {
  return `hookid:${encodeURIComponent(hookId)}`;
}

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

  it('rejects malformed retention admin requests before state mutation', async () => {
    harness = await startHarness({ secret: 'retention-secret', virtualClock: true });
    const world = createCelldWorld({
      fleetUrl: harness.url,
      secret: 'retention-secret',
      deploymentId: 'retention-tests',
    });
    const runId = await createCompletedRun(world, 'invalid-admin');
    await finishRun(world, runId);
    const run = harness.fleet.cell('runs', runId).instance as WorkflowRunDO;
    const storage = harness.fleet.cell('runs', runId).storage;
    await driveTerminalCleanup(harness, runId);
    const before = structuredClone(Array.from(storage.data.entries()));
    storage.resetOperationCounts();

    for (const request of [
      { retentionMs: 0 },
      { retentionMs: -1 },
      { retentionMs: 1.5 },
      { retentionMs: Number.NaN },
      { retentionMs: Number.MAX_SAFE_INTEGER + 1 },
    ]) {
      await expect(run.scheduleCleanup(request)).rejects.toThrow(/retention/);
      await expect(run.cleanupNow(request)).rejects.toThrow(/retention/);
    }

    expect(Array.from(storage.data.entries())).toEqual(before);
    expect(storage.operationCounts.transaction).toBe(0);
  });

  it('rejects invalid event cleanup metadata before creating a run', async () => {
    const fleet = new FakeFleet({ runs: WorkflowRunDO });
    const runId = 'wrun_invalid_cleanup_metadata';
    const run = fleet.namespace('runs').get({ toString: () => runId }) as WorkflowRunDO;
    await expect(
      run.applyEvent({
        runId,
        data: {
          eventType: 'run_created',
          eventData: {
            deploymentId: 'retention-tests',
            workflowName: 'invalid-cleanup',
            input: [],
          },
        },
        cleanup: { retentionMs: -1 },
      }),
    ).rejects.toThrow(/retentionMs/);
    const storage = fleet.cell('runs', runId).storage;
    expect(storage.data.size).toBe(0);
    expect(storage.operationCounts.transaction).toBe(0);
  });

  it('purges payloads, indexes, streams, and queued work without allowing resurrection', async () => {
    process.env.CELLD_QUEUE_MODE = 'native';
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
    expect(harness.queueMessages).toHaveLength(1);
    expect(harness.queuePayloads.size).toBe(1);
    await expect(
      world.queue(
        '__wkf_workflow_retention',
        { runId },
        { idempotencyKey: `late-before-cleanup:${runId}` },
      ),
    ).rejects.toSatisfy((error) => RunExpiredError.is(error));
    const status = await driveCleanup(harness, world, runId);
    expect(status).toMatchObject({
      phase: 'tombstoned',
      deletedStreams: 1,
      deletedQueuePayloads: 1,
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
    expect(harness.queuePayloads.size).toBe(0);

    const runKeys = Array.from(harness.fleet.cell('runs', runId).storage.data.keys()).toSorted();
    expect(runKeys).toEqual(['retention:tombstone']);
    const run = harness.fleet.cell('runs', runId).instance as WorkflowRunDO;
    await expect(
      run.resolveHookTokenClaim({
        hookId: 'late-hook-after-tombstone',
        token: 'late-token-after-tombstone',
        claimId: 'late-claim-after-tombstone',
      }),
    ).resolves.toEqual({ committed: false });
    expect(Array.from(harness.fleet.cell('runs', runId).storage.data.keys())).toEqual([
      'retention:tombstone',
    ]);
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

    const indexSlot = harness.fleet.cell('run-catalog', runCatalogShardName(runId));
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

    const entries = await workflowIndex(harness).listRuns({ prefix: 'run:retention-retry:' });
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
      await seedHookIndex(harness, hook);
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

    expect(hookCounts).toEqual([86, 22, 0, 0, 0, 0, 0]);
    expect(waitCounts).toEqual([300, 300, 300, 300, 172, 44, 0]);
    expect(runStorage.data.has('terminal:cleanup')).toBe(false);
    expect(runStorage.alarmAt).toBeNull();
    expect(
      Array.from({ length: 150 }, (_, hookIndex) => {
        const token = `token-${String(hookIndex).padStart(3, '0')}`;
        return harness.fleet
          .cell('hook-tokens', hookTokenShardName(token))
          .storage.data.has(hookTokenRecordKey(token));
      }),
    ).not.toContain(true);
    await expect(
      (harness.fleet.cell('runs', runId).instance as WorkflowRunDO).getLifecycleStatus(),
    ).resolves.toBe('terminal');
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
    await seedHookIndex(harness, hook);
    await finishRun(world, runId);

    const tokenStorage = harness.fleet.cell('hook-tokens', hookTokenShardName(token)).storage;
    const idStorage = harness.fleet.cell('hook-ids', hookIdShardName(hookId)).storage;
    tokenStorage.failNextMutation(
      (mutation) => mutation.operation === 'delete' && mutation.key === hookTokenRecordKey(token),
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
    expect(tokenStorage.data.has(hookTokenRecordKey(token))).toBe(true);

    harness.fleet.advance(1_000);
    await harness.fleet.fireDueAlarms();
    harness.fleet.advance(1);
    await harness.fleet.fireDueAlarms();
    harness.fleet.advance(1);
    await harness.fleet.fireDueAlarms();
    expect(runStorage.data.has('terminal:cleanup')).toBe(false);
    expect(runStorage.data.has(`hook:${hookId}`)).toBe(false);
    expect(tokenStorage.data.has(hookTokenRecordKey(token))).toBe(false);
    expect(idStorage.data.has(hookIdRecordKey(hookId))).toBe(false);
  });

  it('uses exact claims for disposed hooks without accumulating run fences', async () => {
    harness = await startHarness({ secret: 'retention-secret', virtualClock: true });
    const world = createCelldWorld({
      fleetUrl: harness.url,
      secret: 'retention-secret',
      deploymentId: 'retention-tests',
    });
    const runId = await createCompletedRun(world, 'disposed-hook-fences');
    const hookId = 'disposed-before-terminal';
    const token = 'disposed-before-terminal-token';

    await world.events.create(runId, {
      eventType: 'hook_created',
      correlationId: hookId,
      eventData: { token },
    });
    await world.events.create(runId, {
      eventType: 'hook_disposed',
      correlationId: hookId,
    });

    const tokenStorage = harness.fleet.cell('hook-tokens', hookTokenShardName(token)).storage;
    const idStorage = harness.fleet.cell('hook-ids', hookIdShardName(hookId)).storage;
    expect(Array.from(tokenStorage.data.keys()).filter((key) => key.startsWith('fence:'))).toEqual(
      [],
    );
    expect(Array.from(idStorage.data.keys()).filter((key) => key.startsWith('fence:'))).toEqual([]);

    await finishRun(world, runId);
    await driveTerminalCleanup(harness, runId);

    expect(tokenStorage.data.has(`runfence:${encodeURIComponent(runId)}`)).toBe(false);
    expect(idStorage.data.has(`runfence:${encodeURIComponent(runId)}`)).toBe(false);
  });

  it('holds an exact queue claim until its workflow retry deadline', async () => {
    let fleet!: FakeFleet;
    fleet = new FakeFleet({ runs: WorkflowRunDO }, { clock: () => fleet.now });
    const claim = fleet.namespace('runs').get({
      toString: () => 'claim:queue:test-key',
    }) as WorkflowRunDO;

    await expect(claim.claimInflight({ messageId: 'msg_a', staleMs: 1_000 })).resolves.toEqual({
      claimed: true,
    });
    await expect(claim.claimInflight({ messageId: 'msg_a', staleMs: 1_000 })).resolves.toEqual({
      claimed: false,
      retryAt: fleet.now + 1_000,
    });
    await expect(claim.claimInflight({ messageId: 'msg_b', staleMs: 1_000 })).resolves.toEqual({
      claimed: false,
    });

    await expect(
      claim.holdInflight({
        messageId: 'msg_a',
        retryAt: fleet.now + 50,
        expiresAt: fleet.now + 500,
      }),
    ).resolves.toEqual({ held: true });
    await expect(claim.claimInflight({ messageId: 'msg_a', staleMs: 1_000 })).resolves.toEqual({
      claimed: false,
      retryAt: fleet.now + 50,
    });
    fleet.advance(50);
    await expect(claim.claimInflight({ messageId: 'msg_a', staleMs: 1_000 })).resolves.toEqual({
      claimed: true,
    });
  });

  it('reserves one queue message per idempotency key until completion', async () => {
    let fleet!: FakeFleet;
    fleet = new FakeFleet({ runs: WorkflowRunDO }, { clock: () => fleet.now });
    const claim = fleet.namespace('runs').get({
      toString: () => 'claim:queue:reservation-key',
    }) as WorkflowRunDO;

    await expect(
      claim.reserveQueueMessage({ messageId: 'msg_reserved', expiresAt: fleet.now + 5_000 }),
    ).resolves.toEqual({ admitted: true, messageId: 'msg_reserved' });
    await expect(
      claim.reserveQueueMessage({ messageId: 'msg_duplicate', expiresAt: fleet.now + 5_000 }),
    ).resolves.toEqual({ admitted: false, messageId: 'msg_reserved' });

    await claim.completeQueueMessage('msg_reserved');
    await expect(
      claim.reserveQueueMessage({ messageId: 'msg_after_ack', expiresAt: fleet.now + 5_000 }),
    ).resolves.toEqual({ admitted: true, messageId: 'msg_after_ack' });
  });

  it('cleans an ambiguously published object-store payload through its orphan alarm', async () => {
    const objects = new Map([['workflow-queue/wrun_orphan/msg_orphan', 'payload']]);
    const deleted: string[] = [];
    const cellEnv: Record<string, unknown> = {};
    const fleet = new FakeFleet({ runs: WorkflowRunDO }, cellEnv);
    Object.assign(cellEnv, {
      clock: () => fleet.now,
      WORKFLOW_DB: fleet.namespace('runs'),
      WORKFLOW_QUEUE_PAYLOADS: {
        delete: async (keys: string | string[]) => {
          for (const key of Array.isArray(keys) ? keys : [keys]) {
            objects.delete(key);
            deleted.push(key);
          }
        },
      },
    });
    const runId = 'wrun_orphan';
    const messageId = 'msg_orphan';
    const key = 'workflow-queue/wrun_orphan/msg_orphan';
    const run = fleet.namespace('runs').get({ toString: () => runId }) as WorkflowRunDO;
    const orphanName = queueOrphanName(messageId);
    const orphan = fleet.namespace('runs').get({ toString: () => orphanName }) as WorkflowRunDO;
    await run.registerQueuePayload({
      messageId,
      key,
      orphanExpiresAt: fleet.now + 10,
    });
    await orphan.scheduleQueuePayloadOrphan({
      messageId,
      runId,
      key,
      expiresAt: fleet.now + 10,
    });

    fleet.advance(10);
    await fleet.fireDueAlarms();

    expect(deleted).toEqual([key]);
    expect(objects.size).toBe(0);
    expect(fleet.cell('runs', runId).storage.data.has(queuePayloadRegistryKey(messageId))).toBe(
      false,
    );
    expect(fleet.cell('runs', orphanName).storage.data.size).toBe(0);
  });

  it('rejects an orphan extension after cleanup has acquired its deletion lease', async () => {
    const deleteStarted = Promise.withResolvers<void>();
    const releaseDelete = Promise.withResolvers<void>();
    let fleet!: FakeFleet;
    const cellEnv: Record<string, unknown> = {};
    fleet = new FakeFleet({ runs: WorkflowRunDO }, cellEnv);
    Object.assign(cellEnv, {
      clock: () => fleet.now,
      WORKFLOW_DB: fleet.namespace('runs'),
      WORKFLOW_QUEUE_PAYLOADS: {
        delete: async () => {
          deleteStarted.resolve();
          await releaseDelete.promise;
        },
      },
    });
    const messageId = 'msg_orphan_extension_race';
    const orphan = fleet.namespace('runs').get({
      toString: () => queueOrphanName(messageId),
    }) as WorkflowRunDO;
    const scheduled = {
      messageId,
      runId: 'wrun_orphan_extension_race',
      key: 'workflow-queue/wrun_orphan_extension_race/msg_orphan_extension_race',
      expiresAt: fleet.now,
    };
    await orphan.scheduleQueuePayloadOrphan(scheduled);

    const cleanup = orphan.alarm();
    await deleteStarted.promise;
    try {
      await expect(
        orphan.scheduleQueuePayloadOrphan({ ...scheduled, expiresAt: fleet.now + 10_000 }),
      ).rejects.toThrow(/deletion.*progress/i);
    } finally {
      releaseDelete.resolve();
      await cleanup;
    }

    expect(fleet.cell('runs', queueOrphanName(messageId)).storage.data.size).toBe(0);
  });

  it('deletes object-store queue payloads in bounded, generation-safe pages', async () => {
    let fleet!: FakeFleet;
    let releaseFirst!: () => void;
    const firstStarted = Promise.withResolvers<void>();
    const firstRelease = new Promise<void>((resolve) => {
      releaseFirst = resolve;
    });
    let calls = 0;
    const remove = vi.fn<(_keys: string | string[]) => Promise<void>>(async () => {
      calls++;
      if (calls === 1) {
        firstStarted.resolve();
        await firstRelease;
      }
    });
    fleet = new FakeFleet(
      { runs: WorkflowRunDO },
      { clock: () => fleet.now, WORKFLOW_QUEUE_PAYLOADS: { delete: remove } },
    );
    const runId = 'wrun_concurrent_queue_payload_cleanup';
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
      phase: 'queues',
      generation: 0,
      attempts: 0,
      deletedPayloadKeys: 0,
      deletedStreams: 0,
      deletedQueuePayloads: 0,
    } satisfies CleanupRecord);
    for (let index = 0; index < 130; index++) {
      const messageId = `msg_${String(index).padStart(3, '0')}`;
      storage.data.set(queuePayloadRegistryKey(messageId), {
        messageId,
        key: `queue/${messageId}`,
        orphanExpiresAt: fleet.now + 1_000,
      });
    }

    const first = run.cleanupNow({ retentionMs: 1 });
    await firstStarted.promise;
    const second = run.cleanupNow({ retentionMs: 1 });
    await vi.waitFor(() => expect(remove).toHaveBeenCalledTimes(2));
    await second;
    releaseFirst();
    await first;

    expect(storage.data.get(CLEANUP_RECORD_KEY)).toMatchObject({
      phase: 'queues',
      deletedQueuePayloads: 128,
      attempts: 0,
    });
    await run.cleanupNow({ retentionMs: 1 });
    expect(storage.data.get(CLEANUP_RECORD_KEY)).toMatchObject({
      phase: 'payload',
      deletedQueuePayloads: 130,
    });
  });
});
