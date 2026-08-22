import { RunExpiredError } from '@workflow/errors';
import { afterEach, describe, expect, it } from 'vitest';
import { createCelldWorld } from '../src/index.js';
import { runCatalogShardName } from '../src/indexes.js';
import { sortableTimestamp } from '../src/retention.js';
import { startHarness, type Harness } from '../src/testing/http-harness.js';
import type { QueueDO } from '../src/worker/durable-objects/QueueDO.js';
import type { WorkflowRunDO } from '../src/worker/durable-objects/WorkflowRunDO.js';
import { runRetentionSweep, type RetentionSweepEnv } from '../src/worker/retention-sweep.js';

function sweepEnv(
  harness: Harness,
  settings: Pick<
    RetentionSweepEnv,
    'WORKFLOW_RETENTION_MS' | 'WORKFLOW_RETENTION_BATCH_SIZE' | 'WORKFLOW_RETENTION_QUEUE_SHARDS'
  >,
): RetentionSweepEnv {
  return {
    WORKFLOW_DB: harness.fleet.namespace('runs') as RetentionSweepEnv['WORKFLOW_DB'],
    WORKFLOW_RUN_CATALOG: harness.fleet.namespace(
      'run-catalog',
    ) as RetentionSweepEnv['WORKFLOW_RUN_CATALOG'],
    WORKFLOW_HOOK_TOKENS: harness.fleet.namespace(
      'hook-tokens',
    ) as RetentionSweepEnv['WORKFLOW_HOOK_TOKENS'],
    WORKFLOW_HOOK_IDS: harness.fleet.namespace(
      'hook-ids',
    ) as RetentionSweepEnv['WORKFLOW_HOOK_IDS'],
    ...settings,
  };
}

async function createRun(
  world: ReturnType<typeof createCelldWorld>,
  suffix: string,
  status: 'pending' | 'running' | 'completed',
): Promise<string> {
  const created = await world.events.create(null, {
    eventType: 'run_created',
    eventData: {
      deploymentId: 'retention-sweep-tests',
      workflowName: `retention-sweep-${suffix}`,
      input: [suffix],
    },
  });
  if (status === 'running' || status === 'completed') {
    await world.events.create(created.run.runId, { eventType: 'run_started' });
  }
  if (status === 'completed') {
    await world.events.create(created.run.runId, {
      eventType: 'run_completed',
      eventData: { output: ['done'] },
    });
  }
  return created.run.runId;
}

async function driveCleanup(harness: Harness, runIds: string[]): Promise<void> {
  for (let page = 0; page < 30; page++) {
    if (
      runIds.every(
        (runId) =>
          harness.fleet.cell('runs', runId).storage.data.has('retention:tombstone') &&
          !harness.fleet.cell('runs', runId).storage.data.has('retention:cleanup'),
      )
    ) {
      return;
    }
    harness.fleet.advance(1);
    await harness.fleet.fireDueAlarms();
  }
  throw new Error('maximum-age cleanup did not finish');
}

describe('fleet-wide workflow retention sweep', () => {
  let harness: Harness | undefined;

  afterEach(async () => {
    delete process.env.CELLD_QUEUE_MODE;
    await harness?.close();
    harness = undefined;
  });

  it('expires pending, running, and terminal workflows by creation age', async () => {
    process.env.CELLD_QUEUE_MODE = 'cells';
    harness = await startHarness({ secret: 'retention-sweep-secret', virtualClock: true });
    const world = createCelldWorld({
      fleetUrl: harness.url,
      secret: 'retention-sweep-secret',
      deploymentId: 'retention-sweep-tests',
      baseUrl: 'http://127.0.0.1:1',
      queueShards: 2,
    });
    const pending = await createRun(world, 'pending', 'pending');
    const running = await createRun(world, 'running', 'running');
    const completed = await createRun(world, 'completed', 'completed');
    await world.queue(
      '__wkf_workflow_retention_sweep',
      { runId: running },
      { delaySeconds: 3_600, idempotencyKey: `retention:${running}` },
    );

    harness.fleet.advance(2_000);
    const young = await createRun(world, 'young', 'pending');
    const result = await runRetentionSweep(
      harness.fleet.now,
      sweepEnv(harness, {
        WORKFLOW_RETENTION_MS: 1_000,
        // New runs persist their application-side shard count, so this is
        // only a fallback for runs created before the retention sweep exists.
        WORKFLOW_RETENTION_QUEUE_SHARDS: 1,
      }),
    );

    expect(result).toMatchObject({
      disabled: false,
      scanned: 3,
      expired: 3,
      scheduled: 0,
      missing: 0,
      notDue: 0,
    });
    for (const runId of [pending, running, completed]) {
      await expect(world.runs.get(runId)).rejects.toSatisfy((error) => RunExpiredError.is(error));
      expect(await world.retention.getStatus(runId)).toMatchObject({
        reason: 'maximum-age',
        phase: 'streams',
      });
    }
    await expect(world.runs.get(young)).resolves.toMatchObject({ status: 'pending' });

    await driveCleanup(harness, [pending, running, completed]);
    expect(
      harness.fleet.cell('runs', pending).storage.data.get('retention:tombstone'),
    ).toMatchObject({ status: 'pending', cleanup: { reason: 'maximum-age' } });
    expect(
      harness.fleet.cell('runs', running).storage.data.get('retention:tombstone'),
    ).toMatchObject({ status: 'running', cleanup: { reason: 'maximum-age' } });
    expect(
      harness.fleet.cell('runs', completed).storage.data.get('retention:tombstone'),
    ).toMatchObject({ status: 'completed', terminalStatus: 'completed' });
    for (let shard = 0; shard < 2; shard++) {
      const queue = harness.fleet.namespace('queue').get({
        toString: () => `q:${shard}`,
      }) as QueueDO;
      await expect(queue.stats()).resolves.toMatchObject({
        pending: 0,
        inflight: 0,
        deadLetters: 0,
      });
    }
  });

  it('makes progress across bounded cron occurrences', async () => {
    harness = await startHarness({ secret: 'retention-sweep-secret', virtualClock: true });
    const world = createCelldWorld({
      fleetUrl: harness.url,
      secret: 'retention-sweep-secret',
      deploymentId: 'retention-sweep-tests',
    });
    const runIds = await Promise.all(
      Array.from({ length: 5 }, (_, index) => createRun(world, `batch-${index}`, 'pending')),
    );
    harness.fleet.advance(1_000);
    const env = sweepEnv(harness, {
      WORKFLOW_RETENTION_MS: 100,
      WORKFLOW_RETENTION_BATCH_SIZE: 2,
      WORKFLOW_RETENTION_QUEUE_SHARDS: 1,
    });

    await expect(runRetentionSweep(harness.fleet.now, env)).resolves.toMatchObject({
      scanned: 2,
      expired: 2,
    });
    await expect(runRetentionSweep(harness.fleet.now, env)).resolves.toMatchObject({
      scanned: 2,
      expired: 2,
    });
    await expect(runRetentionSweep(harness.fleet.now, env)).resolves.toMatchObject({
      scanned: 1,
      expired: 1,
    });
    await expect(runRetentionSweep(harness.fleet.now, env)).resolves.toMatchObject({
      scanned: 0,
    });

    for (const runId of runIds) {
      await expect(world.runs.get(runId)).rejects.toSatisfy((error) => RunExpiredError.is(error));
    }
  });

  it('uses the earlier terminal or maximum-age deadline', async () => {
    harness = await startHarness({ secret: 'retention-sweep-secret', virtualClock: true });
    const world = createCelldWorld({
      fleetUrl: harness.url,
      secret: 'retention-sweep-secret',
      deploymentId: 'retention-sweep-tests',
      runRetentionMs: 10_000,
    });
    const runId = await createRun(world, 'earliest-deadline', 'completed');
    const terminalDeadline = (await world.retention.getStatus(runId))?.dueAt.getTime();
    harness.fleet.advance(2_000);

    await runRetentionSweep(
      harness.fleet.now,
      sweepEnv(harness, {
        WORKFLOW_RETENTION_MS: 1_000,
        WORKFLOW_RETENTION_QUEUE_SHARDS: 1,
      }),
    );
    const status = await world.retention.getStatus(runId);
    expect(status).toMatchObject({ reason: 'maximum-age', phase: 'streams' });
    expect(status?.dueAt.getTime()).toBeLessThan(terminalDeadline ?? 0);
  });

  it('rechecks a catalog candidate against the authoritative creation time', async () => {
    harness = await startHarness({ secret: 'retention-sweep-secret', virtualClock: true });
    const world = createCelldWorld({
      fleetUrl: harness.url,
      secret: 'retention-sweep-secret',
      deploymentId: 'retention-sweep-tests',
    });
    const runId = await createRun(world, 'authoritative-cutoff', 'pending');
    const run = await world.runs.get(runId);
    const falseCreatedAt = new Date(run.createdAt.getTime() - 10_000);
    const falseKey = `runall:${sortableTimestamp(falseCreatedAt)}:${runId}`;
    const catalog = harness.fleet.cell('run-catalog', runCatalogShardName(runId)).storage.data;
    catalog.set(falseKey, JSON.stringify({ runId }));

    await expect(
      runRetentionSweep(
        harness.fleet.now,
        sweepEnv(harness, {
          WORKFLOW_RETENTION_MS: 1_000,
          WORKFLOW_RETENTION_QUEUE_SHARDS: 1,
        }),
      ),
    ).resolves.toMatchObject({ scanned: 1, notDue: 1, expired: 0 });
    await expect(world.runs.get(runId)).resolves.toMatchObject({ status: 'pending' });
    await expect(world.retention.getStatus(runId)).resolves.toBeNull();
    expect(catalog.has(falseKey)).toBe(false);
    await expect(
      runRetentionSweep(
        harness.fleet.now,
        sweepEnv(harness, {
          WORKFLOW_RETENTION_MS: 1_000,
          WORKFLOW_RETENTION_QUEUE_SHARDS: 1,
        }),
      ),
    ).resolves.toMatchObject({ scanned: 0 });
  });

  it('advances past a full page of rejected catalog candidates', async () => {
    harness = await startHarness({ secret: 'retention-sweep-secret', virtualClock: true });
    const world = createCelldWorld({
      fleetUrl: harness.url,
      secret: 'retention-sweep-secret',
      deploymentId: 'retention-sweep-tests',
    });
    const expiredRunId = await createRun(world, 'stale-page-expired', 'pending');
    const expiredRun = await world.runs.get(expiredRunId);
    harness.fleet.advance(2_000);
    const youngRunId = await createRun(world, 'stale-page-young', 'pending');
    const staleEntries = [
      {
        runId: youngRunId,
        key: `runall:${sortableTimestamp(new Date(expiredRun.createdAt.getTime() - 20_000))}:${youngRunId}`,
      },
      {
        runId: 'wrun_missing_retention_candidate',
        key: `runall:${sortableTimestamp(new Date(expiredRun.createdAt.getTime() - 10_000))}:wrun_missing_retention_candidate`,
      },
    ];
    for (const entry of staleEntries) {
      harness.fleet
        .cell('run-catalog', runCatalogShardName(entry.runId))
        .storage.data.set(entry.key, JSON.stringify({ runId: entry.runId }));
    }
    const env = sweepEnv(harness, {
      WORKFLOW_RETENTION_MS: 1_000,
      WORKFLOW_RETENTION_BATCH_SIZE: 2,
      WORKFLOW_RETENTION_QUEUE_SHARDS: 1,
    });

    await expect(runRetentionSweep(harness.fleet.now, env)).resolves.toMatchObject({
      scanned: 2,
      expired: 0,
      missing: 1,
      notDue: 1,
    });
    for (const entry of staleEntries) {
      expect(
        harness.fleet
          .cell('run-catalog', runCatalogShardName(entry.runId))
          .storage.data.has(entry.key),
      ).toBe(false);
    }
    await expect(runRetentionSweep(harness.fleet.now, env)).resolves.toMatchObject({
      scanned: 1,
      expired: 1,
      missing: 0,
      notDue: 0,
    });
    await expect(world.runs.get(expiredRunId)).rejects.toSatisfy((error) =>
      RunExpiredError.is(error),
    );
  });

  it('keeps successful progress when one candidate is retried by cron', async () => {
    harness = await startHarness({ secret: 'retention-sweep-secret', virtualClock: true });
    const world = createCelldWorld({
      fleetUrl: harness.url,
      secret: 'retention-sweep-secret',
      deploymentId: 'retention-sweep-tests',
    });
    const retryRunId = await createRun(world, 'retry-candidate', 'pending');
    harness.fleet.advance(1);
    const successfulRunId = await createRun(world, 'successful-candidate', 'pending');
    harness.fleet.advance(1_000);
    const env = sweepEnv(harness, {
      WORKFLOW_RETENTION_MS: 100,
      WORKFLOW_RETENTION_BATCH_SIZE: 2,
      WORKFLOW_RETENTION_QUEUE_SHARDS: 1,
    });
    const retryRun = harness.fleet.cell('runs', retryRunId).instance as WorkflowRunDO;
    const originalEnforce = retryRun.enforceRetention.bind(retryRun);
    retryRun.enforceRetention = async () => {
      throw new Error('injected retention admission failure');
    };

    await expect(runRetentionSweep(harness.fleet.now, env)).rejects.toThrow(
      /retention sweep failed for 1 run/,
    );
    await expect(world.runs.get(retryRunId)).resolves.toMatchObject({ status: 'pending' });
    await expect(world.runs.get(successfulRunId)).rejects.toSatisfy((error) =>
      RunExpiredError.is(error),
    );

    retryRun.enforceRetention = originalEnforce;
    await expect(runRetentionSweep(harness.fleet.now, env)).resolves.toMatchObject({
      scanned: 1,
      expired: 1,
    });
    await expect(world.runs.get(retryRunId)).rejects.toSatisfy((error) =>
      RunExpiredError.is(error),
    );
  });

  it('does no catalog work while the policy is disabled', async () => {
    harness = await startHarness({ secret: 'retention-sweep-secret', virtualClock: true });
    const result = await runRetentionSweep(
      harness.fleet.now,
      sweepEnv(harness, { WORKFLOW_RETENTION_MS: 0 }),
    );
    expect(result).toEqual({
      disabled: true,
      cutoff: null,
      scanned: 0,
      scheduled: 0,
      expired: 0,
      missing: 0,
      notDue: 0,
    });
  });
});
