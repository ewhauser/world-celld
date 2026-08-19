import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import {
  allRunCatalogShardNames,
  hookIdShardName,
  hookTokenShardName,
  runCatalogShardName,
  runFenceCellName,
} from '../../src/indexes.js';
import { createRemoteEnv } from '../../src/remote/namespaces.js';
import { createStorage } from '../../src/storage.js';
import type { FakeStorage, FakeStorageOperationCounts } from '../../src/testing/fake-cell.js';
import { startHarness, type Harness } from '../../src/testing/http-harness.js';

const SECRET = 'index-scalability-secret';
const WORKLOAD = 48;
const CONCURRENCY = 16;
const SYNTHETIC_TRANSACTION_MS = 2;

interface PublicMetric {
  publicRpcs: number;
  paths: Record<string, number>;
  elapsedMs: number;
}

function percentile(values: number[], fraction: number): number {
  const ordered = values.toSorted((left, right) => left - right);
  return ordered[Math.min(ordered.length - 1, Math.floor(ordered.length * fraction))] ?? 0;
}

async function runPool(
  values: string[],
  concurrency: number,
  operation: (value: string, index: number) => Promise<void>,
): Promise<number[]> {
  const latencies = Array.from<number>({ length: values.length });
  let next = 0;
  await Promise.all(
    Array.from({ length: concurrency }, async () => {
      for (;;) {
        const index = next++;
        if (index >= values.length) return;
        const startedAt = performance.now();
        await operation(values[index], index);
        latencies[index] = performance.now() - startedAt;
      }
    }),
  );
  return latencies;
}

function emptyCounts(): FakeStorageOperationCounts {
  return {
    get: 0,
    getMany: 0,
    list: 0,
    put: 0,
    putMany: 0,
    delete: 0,
    deleteMany: 0,
    transaction: 0,
  };
}

function storageCounts(storages: FakeStorage[]): FakeStorageOperationCounts {
  const result = emptyCounts();
  for (const storage of storages) {
    for (const key of Object.keys(result) as (keyof FakeStorageOperationCounts)[]) {
      result[key] += storage.operationCounts[key];
    }
  }
  return result;
}

function reset(storages: FakeStorage[]): void {
  for (const storage of storages) storage.resetOperationCounts();
}

function resultMetric(workload: number, elapsedMs: number, latencies: number[]) {
  return {
    elapsedMs,
    throughputPerSecond: (workload * 1000) / elapsedMs,
    p50Ms: percentile(latencies, 0.5),
    p95Ms: percentile(latencies, 0.95),
  };
}

describe('sharded index scalability evidence', () => {
  let harness: Harness;

  beforeAll(async () => {
    harness = await startHarness({ secret: SECRET, virtualClock: true });
  });

  afterAll(async () => {
    await harness.close();
  });

  it('records public RPC fanout and internal storage work by index caller', async () => {
    let publicRpcs = 0;
    const paths = new Map<string, number>();
    const countedFetch: typeof fetch = async (input, init) => {
      const path = new URL(input instanceof Request ? input.url : String(input)).pathname;
      publicRpcs++;
      paths.set(path, (paths.get(path) ?? 0) + 1);
      return fetch(input, init);
    };
    const env = createRemoteEnv({ fleetUrl: harness.url, secret: SECRET, fetchImpl: countedFetch });
    const storage = createStorage({
      env: { WORKFLOW_DB: env.WORKFLOW_DB, WORKFLOW_INDEX: env.WORKFLOW_INDEX },
      deploymentId: 'index-scalability',
    });
    const runId = 'wrun_sharded_fanout_evidence';
    const catalogStorages = allRunCatalogShardNames().map(
      (name) => harness.fleet.cell('run-catalog', name).storage,
    );
    const runFenceStorage = harness.fleet.cell('run-fences', runFenceCellName(runId)).storage;
    const tokenStorage = harness.fleet.cell(
      'hook-tokens',
      hookTokenShardName('evidence-token'),
    ).storage;
    const idStorage = harness.fleet.cell('hook-ids', hookIdShardName('evidence-hook')).storage;

    const measure = async (operation: () => Promise<unknown>): Promise<PublicMetric> => {
      publicRpcs = 0;
      paths.clear();
      const startedAt = performance.now();
      await operation();
      return {
        publicRpcs,
        paths: Object.fromEntries(
          [...paths].toSorted(([left], [right]) => left.localeCompare(right)),
        ),
        elapsedMs: performance.now() - startedAt,
      };
    };
    const indexCounts = () => ({
      runCatalog: storageCounts(catalogStorages),
      runFence: storageCounts([runFenceStorage]),
      hookToken: storageCounts([tokenStorage]),
      hookId: storageCounts([idStorage]),
    });
    const resetIndexes = () =>
      reset([...catalogStorages, runFenceStorage, tokenStorage, idStorage]);

    resetIndexes();
    const created = await measure(() =>
      storage.events.create(runId, {
        eventType: 'run_created',
        eventData: {
          deploymentId: 'index-scalability',
          workflowName: 'index-evidence',
          input: [],
        },
      }),
    );
    const createIndexOperations = indexCounts();

    resetIndexes();
    const updated = await measure(() =>
      storage.events.create(runId, {
        eventType: 'attr_set',
        correlationId: 'evidence-attr',
        eventData: {
          changes: [{ key: 'evidence', value: 'updated' }],
          writer: { type: 'workflow' },
        },
      }),
    );
    const updateIndexOperations = indexCounts();

    resetIndexes();
    const listed = await measure(() =>
      storage.runs.list({ workflowName: 'index-evidence', pagination: { limit: 20 } }),
    );
    const listIndexOperations = indexCounts();

    resetIndexes();
    const hookCreated = await measure(() =>
      storage.events.create(runId, {
        eventType: 'hook_created',
        correlationId: 'evidence-hook',
        eventData: { token: 'evidence-token' },
      }),
    );
    const hookCreateIndexOperations = indexCounts();

    resetIndexes();
    const getByToken = await measure(() => storage.hooks.getByToken('evidence-token'));
    const tokenReadIndexOperations = indexCounts();

    resetIndexes();
    const getById = await measure(() => storage.hooks.get('evidence-hook'));
    const idReadIndexOperations = indexCounts();

    resetIndexes();
    const resumed = await measure(() =>
      storage.events.create(runId, {
        eventType: 'hook_received',
        correlationId: 'evidence-hook',
        eventData: { payload: 'ok' },
      }),
    );
    const resumeIndexOperations = indexCounts();

    resetIndexes();
    const disposed = await measure(() =>
      storage.events.create(runId, {
        eventType: 'hook_disposed',
        correlationId: 'evidence-hook',
      }),
    );
    const disposeIndexOperations = indexCounts();

    for (let hook = 0; hook < 5; hook++) {
      await storage.events.create(runId, {
        eventType: 'hook_created',
        correlationId: `terminal-hook-${hook}`,
        eventData: { token: `terminal-token-${hook}` },
      });
    }
    resetIndexes();
    const terminal = await measure(() =>
      storage.events.create(runId, {
        eventType: 'run_completed',
        eventData: { output: [] },
      }),
    );
    const terminalIndexOperations = indexCounts();

    const report = {
      source: 'sharded-worktree',
      operations: {
        runCreate: { ...created, indexStorage: createIndexOperations },
        runUpdate: { ...updated, indexStorage: updateIndexOperations },
        runListOne: { ...listed, indexStorage: listIndexOperations },
        hookCreate: { ...hookCreated, indexStorage: hookCreateIndexOperations },
        hookGetByToken: { ...getByToken, indexStorage: tokenReadIndexOperations },
        hookGetById: { ...getById, indexStorage: idReadIndexOperations },
        hookResume: { ...resumed, indexStorage: resumeIndexOperations },
        hookDelete: { ...disposed, indexStorage: disposeIndexOperations },
        terminalFiveHooks: { ...terminal, indexStorage: terminalIndexOperations },
      },
    };
    console.log(`INDEX_SCALABILITY_FANOUT ${JSON.stringify(report)}`);

    expect(created.publicRpcs).toBe(2);
    expect(updated.publicRpcs).toBe(2);
    expect(listed.publicRpcs).toBe(2);
    expect(hookCreated.publicRpcs).toBe(3);
    expect(getByToken.publicRpcs).toBe(1);
    expect(getById.publicRpcs).toBe(1);
    expect(resumed.publicRpcs).toBe(1);
    expect(disposed.publicRpcs).toBe(2);
    expect(terminal.publicRpcs).toBe(2);
    expect(createIndexOperations.runCatalog).toEqual({
      ...emptyCounts(),
      get: 1,
      putMany: 1,
      transaction: 1,
    });
    expect(updateIndexOperations.runCatalog).toEqual(createIndexOperations.runCatalog);
    expect(listIndexOperations.runCatalog).toEqual({ ...emptyCounts(), list: 16 });
    expect(hookCreateIndexOperations).toEqual({
      runCatalog: emptyCounts(),
      runFence: { ...emptyCounts(), get: 2 },
      hookToken: {
        ...emptyCounts(),
        getMany: 2,
        put: 2,
        delete: 1,
        transaction: 2,
      },
      hookId: {
        ...emptyCounts(),
        getMany: 2,
        put: 2,
        delete: 1,
        transaction: 2,
      },
    });
    expect(tokenReadIndexOperations).toEqual({
      runCatalog: emptyCounts(),
      runFence: { ...emptyCounts(), get: 1 },
      hookToken: { ...emptyCounts(), get: 1 },
      hookId: emptyCounts(),
    });
    expect(idReadIndexOperations).toEqual({
      runCatalog: emptyCounts(),
      runFence: { ...emptyCounts(), get: 1 },
      hookToken: emptyCounts(),
      hookId: { ...emptyCounts(), get: 1 },
    });
    expect(resumeIndexOperations).toEqual({
      runCatalog: emptyCounts(),
      runFence: emptyCounts(),
      hookToken: emptyCounts(),
      hookId: emptyCounts(),
    });
    expect(disposeIndexOperations).toEqual({
      runCatalog: emptyCounts(),
      runFence: emptyCounts(),
      hookToken: {
        ...emptyCounts(),
        getMany: 1,
        deleteMany: 1,
        transaction: 1,
      },
      hookId: {
        ...emptyCounts(),
        getMany: 1,
        deleteMany: 1,
        transaction: 1,
      },
    });
    expect(terminalIndexOperations.runCatalog).toEqual({
      ...emptyCounts(),
      get: 1,
      putMany: 1,
      transaction: 1,
    });
    expect(terminalIndexOperations.runFence).toEqual({
      ...emptyCounts(),
      get: 1,
      put: 1,
      transaction: 1,
    });
  });

  it('separates batched-RPC savings from shard distribution under contention', async () => {
    const env = createRemoteEnv({ fleetUrl: harness.url, secret: SECRET });
    const storage = createStorage({
      env: { WORKFLOW_DB: env.WORKFLOW_DB, WORKFLOW_INDEX: env.WORKFLOW_INDEX },
      deploymentId: 'index-scalability-contention',
    });
    for (const name of allRunCatalogShardNames()) {
      harness.fleet.cell('run-catalog', name).storage.setTransactionDelay(SYNTHETIC_TRANSACTION_MS);
    }

    const distributedIds = Array.from(
      { length: WORKLOAD },
      (_, index) => `wrun_distributed_contention_${index}`,
    );
    const targetShard = runCatalogShardName('same-shard-seed');
    const sameShardIds: string[] = [];
    for (let candidate = 0; sameShardIds.length < WORKLOAD; candidate++) {
      const runId = `wrun_same_shard_contention_${candidate}`;
      if (runCatalogShardName(runId) === targetShard) sameShardIds.push(runId);
    }

    const createRuns = async (runIds: string[]) => {
      const startedAt = performance.now();
      const latencies = await runPool(runIds, CONCURRENCY, async (runId, index) => {
        await storage.events.create(runId, {
          eventType: 'run_created',
          eventData: {
            deploymentId: 'index-scalability-contention',
            workflowName: 'index-contention',
            input: [index],
          },
        });
      });
      const elapsedMs = performance.now() - startedAt;
      return resultMetric(runIds.length, elapsedMs, latencies);
    };

    const catalogStorages = allRunCatalogShardNames().map(
      (name) => harness.fleet.cell('run-catalog', name).storage,
    );
    reset(catalogStorages);
    const sameShardRunCreate = await createRuns(sameShardIds);
    const sameShardIndexStorage = storageCounts(catalogStorages);
    reset(catalogStorages);
    const distributedRunCreate = await createRuns(distributedIds);
    const distributedIndexStorage = storageCounts(catalogStorages);
    const stepStartedAt = performance.now();
    const stepLatencies = await runPool(distributedIds, CONCURRENCY, async (runId, index) => {
      await storage.events.create(runId, {
        eventType: 'step_created',
        correlationId: `step-${index}`,
        eventData: { stepName: `step-${index}`, input: [] },
      });
    });
    const stepElapsedMs = performance.now() - stepStartedAt;

    const report = {
      source: 'sharded-worktree',
      workload: WORKLOAD,
      concurrency: CONCURRENCY,
      syntheticTransactionMs: SYNTHETIC_TRANSACTION_MS,
      sameShardRunCreate,
      sameShardIndexStorage,
      distributedRunCreate,
      distributedIndexStorage,
      stepCreateNoIndex: resultMetric(WORKLOAD, stepElapsedMs, stepLatencies),
    };
    console.log(`INDEX_SCALABILITY_CONTENTION ${JSON.stringify(report)}`);
    expect(distributedRunCreate.throughputPerSecond).toBeGreaterThan(
      sameShardRunCreate.throughputPerSecond,
    );
    const expectedStorage = {
      ...emptyCounts(),
      get: WORKLOAD,
      putMany: WORKLOAD,
      transaction: WORKLOAD,
    };
    expect(sameShardIndexStorage).toEqual(expectedStorage);
    expect(distributedIndexStorage).toEqual(expectedStorage);
  });

  it('records retention cleanup public and internal sharded-index work', async () => {
    let publicRpcs = 0;
    const paths = new Map<string, number>();
    const env = createRemoteEnv({
      fleetUrl: harness.url,
      secret: SECRET,
      fetchImpl: async (input, init) => {
        const path = new URL(input instanceof Request ? input.url : String(input)).pathname;
        publicRpcs++;
        paths.set(path, (paths.get(path) ?? 0) + 1);
        return fetch(input, init);
      },
    });
    const storage = createStorage({
      env: { WORKFLOW_DB: env.WORKFLOW_DB, WORKFLOW_INDEX: env.WORKFLOW_INDEX },
      deploymentId: 'index-retention-evidence',
      runRetentionMs: 1,
      queueShards: 1,
    });
    const runId = 'wrun_sharded_retention_evidence';
    const created = await storage.events.create(runId, {
      eventType: 'run_created',
      eventData: {
        deploymentId: 'index-retention-evidence',
        workflowName: 'index-retention-evidence',
        input: [],
      },
    });
    await storage.events.create(created.run.runId, {
      eventType: 'run_completed',
      eventData: { output: [] },
    });

    const catalog = harness.fleet.namespace('run-catalog').get({
      toString: () => runCatalogShardName(runId),
    }) as { expireRun(...args: unknown[]): Promise<unknown> };
    const fence = harness.fleet.namespace('run-fences').get({
      toString: () => runFenceCellName(runId),
    }) as { fenceExpired(...args: unknown[]): Promise<unknown> };
    const originalExpireRun = catalog.expireRun.bind(catalog);
    const originalFenceExpired = fence.fenceExpired.bind(fence);
    let internalCatalogExpireCalls = 0;
    let internalFenceExpireCalls = 0;
    catalog.expireRun = async (...args) => {
      internalCatalogExpireCalls++;
      return originalExpireRun(...args);
    };
    fence.fenceExpired = async (...args) => {
      internalFenceExpireCalls++;
      return originalFenceExpired(...args);
    };
    const catalogStorage = harness.fleet.cell('run-catalog', runCatalogShardName(runId)).storage;
    const fenceStorage = harness.fleet.cell('run-fences', runFenceCellName(runId)).storage;
    reset([catalogStorage, fenceStorage]);
    publicRpcs = 0;
    paths.clear();
    const startedAt = performance.now();
    try {
      await env.WORKFLOW_DB.get(env.WORKFLOW_DB.idFromName(runId)).cleanupNow({
        retentionMs: 1,
        queueShards: 1,
      });
      for (let page = 0; page < 24; page++) {
        harness.fleet.advance(2);
        await harness.fleet.fireDueAlarms();
        const status = await env.WORKFLOW_DB.get(
          env.WORKFLOW_DB.idFromName(runId),
        ).getCleanupStatus();
        if (status?.phase === 'tombstoned') break;
      }
    } finally {
      catalog.expireRun = originalExpireRun;
      fence.fenceExpired = originalFenceExpired;
    }
    const elapsedMs = performance.now() - startedAt;
    const status = await env.WORKFLOW_DB.get(env.WORKFLOW_DB.idFromName(runId)).getCleanupStatus();
    const report = {
      source: 'sharded-worktree',
      publicRpcs,
      paths: Object.fromEntries(
        [...paths].toSorted(([left], [right]) => left.localeCompare(right)),
      ),
      internalIndexRpcCalls: {
        runFenceExpire: internalFenceExpireCalls,
        runCatalogExpire: internalCatalogExpireCalls,
      },
      indexStorage: {
        runFence: storageCounts([fenceStorage]),
        runCatalog: storageCounts([catalogStorage]),
      },
      elapsedMs,
      finalPhase: status?.phase,
    };
    console.log(`INDEX_SCALABILITY_RETENTION ${JSON.stringify(report)}`);
    expect(internalFenceExpireCalls).toBe(1);
    expect(internalCatalogExpireCalls).toBe(1);
    expect(report.indexStorage.runFence).toEqual({
      ...emptyCounts(),
      get: 3,
      put: 1,
      transaction: 3,
    });
    expect(report.indexStorage.runCatalog).toEqual({
      ...emptyCounts(),
      put: 1,
      deleteMany: 1,
      transaction: 1,
    });
    expect(status?.phase).toBe('tombstoned');
  });
});
