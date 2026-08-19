import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import {
  allRunCatalogShardNames,
  hookIdShardName,
  hookTokenShardName,
  runCatalogShardName,
} from '../../src/indexes.js';
import { createRemoteEnv } from '../../src/remote/namespaces.js';
import { createStorage } from '../../src/storage.js';
import type { FakeStorage, FakeStorageOperationCounts } from '../../src/testing/fake-cell.js';
import { startHarness, type Harness } from '../../src/testing/http-harness.js';
import { QUEUE_FENCE_GRACE_MS } from '../../src/lifecycle.js';
import type { EnqueueRequest } from '../../src/queue.js';
import type { QueueDO } from '../../src/worker/durable-objects/QueueDO.js';

const SECRET = 'index-scalability-secret';
const WORKLOAD = 48;
const CONCURRENCY = 16;
const SYNTHETIC_TRANSACTION_MS = 2;

function queueRequest(messageId: string, runId: string): EnqueueRequest {
  return {
    messageId,
    runId,
    queueName: '__wkf_workflow_lifecycle_evidence',
    pathname: 'flow',
    body: '{}',
    delaySeconds: 3_600,
    config: { targetBaseUrl: 'http://app.invalid', queueShards: 1 },
  };
}

interface PublicMetric {
  publicRpcs: number;
  paths: Record<string, number>;
  internalLifecycleRpcs: number;
  lifecycleStorage: FakeStorageOperationCounts;
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

function countDelta(
  after: FakeStorageOperationCounts,
  before: FakeStorageOperationCounts,
): FakeStorageOperationCounts {
  const result = emptyCounts();
  for (const key of Object.keys(result) as (keyof FakeStorageOperationCounts)[]) {
    result[key] = after[key] - before[key];
  }
  return result;
}

function addCounts(target: FakeStorageOperationCounts, delta: FakeStorageOperationCounts): void {
  for (const key of Object.keys(target) as (keyof FakeStorageOperationCounts)[]) {
    target[key] += delta[key];
  }
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
  let callbackFetches = 0;

  beforeAll(async () => {
    harness = await startHarness({
      secret: SECRET,
      virtualClock: true,
      cellEnv: {
        fetch: async () => {
          callbackFetches++;
          return new Response(null, { status: 204 });
        },
      },
    });
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
    const tokenStorage = harness.fleet.cell(
      'hook-tokens',
      hookTokenShardName('evidence-token'),
    ).storage;
    const idStorage = harness.fleet.cell('hook-ids', hookIdShardName('evidence-hook')).storage;
    const runStorage = harness.fleet.cell('runs', runId).storage;
    let internalLifecycleRpcs = 0;
    let lifecycleStorage = emptyCounts();

    const measure = async (operation: () => Promise<unknown>): Promise<PublicMetric> => {
      publicRpcs = 0;
      paths.clear();
      internalLifecycleRpcs = 0;
      lifecycleStorage = emptyCounts();
      const startedAt = performance.now();
      await operation();
      return {
        publicRpcs,
        paths: Object.fromEntries(
          [...paths].toSorted(([left], [right]) => left.localeCompare(right)),
        ),
        internalLifecycleRpcs,
        lifecycleStorage: { ...lifecycleStorage },
        elapsedMs: performance.now() - startedAt,
      };
    };
    const indexCounts = () => ({
      runCatalog: storageCounts(catalogStorages),
      hookToken: storageCounts([tokenStorage]),
      hookId: storageCounts([idStorage]),
    });
    const resetIndexes = () => reset([...catalogStorages, tokenStorage, idStorage]);

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

    const runInstance = harness.fleet.cell('runs', runId).instance as {
      getLifecycleStatus(): Promise<unknown>;
    };
    const getLifecycleStatus = runInstance.getLifecycleStatus.bind(runInstance);
    runInstance.getLifecycleStatus = async () => {
      internalLifecycleRpcs++;
      const before = { ...runStorage.operationCounts };
      try {
        return await getLifecycleStatus();
      } finally {
        addCounts(lifecycleStorage, countDelta(runStorage.operationCounts, before));
      }
    };

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
    expect(hookCreated).toMatchObject({
      internalLifecycleRpcs: 2,
      lifecycleStorage: { ...emptyCounts(), getMany: 2, transaction: 2 },
    });
    expect(getByToken).toMatchObject({
      internalLifecycleRpcs: 1,
      lifecycleStorage: { ...emptyCounts(), getMany: 1, transaction: 1 },
    });
    expect(getById).toMatchObject({
      internalLifecycleRpcs: 1,
      lifecycleStorage: { ...emptyCounts(), getMany: 1, transaction: 1 },
    });
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
      hookToken: {
        ...emptyCounts(),
        getMany: 2,
        put: 3,
        deleteMany: 1,
        transaction: 2,
      },
      hookId: {
        ...emptyCounts(),
        getMany: 2,
        put: 3,
        deleteMany: 1,
        transaction: 2,
      },
    });
    expect(tokenReadIndexOperations).toEqual({
      runCatalog: emptyCounts(),
      hookToken: { ...emptyCounts(), get: 1 },
      hookId: emptyCounts(),
    });
    expect(idReadIndexOperations).toEqual({
      runCatalog: emptyCounts(),
      hookToken: emptyCounts(),
      hookId: { ...emptyCounts(), get: 1 },
    });
    expect(resumeIndexOperations).toEqual({
      runCatalog: emptyCounts(),
      hookToken: emptyCounts(),
      hookId: emptyCounts(),
    });
    expect(disposeIndexOperations).toEqual({
      runCatalog: emptyCounts(),
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

  it('separates steady-state queue cost from authoritative fallback after compaction', async () => {
    let publicRpcs = 0;
    const countedFetch: typeof fetch = async (input, init) => {
      publicRpcs++;
      return fetch(input, init);
    };
    const env = createRemoteEnv({ fleetUrl: harness.url, secret: SECRET, fetchImpl: countedFetch });
    const activeRunId = 'wrun_queue_steady_state_evidence';
    harness.fleet.cell('runs', activeRunId).storage.data.set('run', {
      runId: activeRunId,
      status: 'running',
    });
    const activeRunStorage = harness.fleet.cell('runs', activeRunId).storage;
    const activeRunInstance = harness.fleet.cell('runs', activeRunId).instance as {
      getLifecycleStatus(): Promise<unknown>;
    };
    const getActiveLifecycleStatus = activeRunInstance.getLifecycleStatus.bind(activeRunInstance);
    let steadyAuthorityRpcs = 0;
    activeRunInstance.getLifecycleStatus = async () => {
      steadyAuthorityRpcs++;
      return getActiveLifecycleStatus();
    };
    const steadyName = 'q:lifecycle-steady';
    const steadyStorage = harness.fleet.cell('queue', steadyName).storage;
    steadyStorage.resetOperationCounts();
    activeRunStorage.resetOperationCounts();
    publicRpcs = 0;
    const steady = await env.WORKFLOW_QUEUE.get(env.WORKFLOW_QUEUE.idFromName(steadyName)).enqueue(
      queueRequest('msg_queue_steady', activeRunId),
    );
    const steadyReport = {
      publicRpcs,
      internalAuthorityRpcs: steadyAuthorityRpcs,
      queueStorage: storageCounts([steadyStorage]),
      authorityStorage: storageCounts([activeRunStorage]),
    };

    const expiredRunId = 'wrun_queue_fallback_evidence';
    const expiredRunStorage = harness.fleet.cell('runs', expiredRunId).storage;
    expiredRunStorage.data.set('retention:tombstone', { runId: expiredRunId });
    const fallbackName = 'q:lifecycle-fallback';
    const fallback = harness.fleet.namespace('queue').get({
      toString: () => fallbackName,
    }) as QueueDO;
    const expiry = await fallback.expireRun(expiredRunId, harness.fleet.now);
    if (!expiry.done) throw new Error('expected final queue expiry receipt');
    await fallback.acknowledgeExpireRun(expiredRunId, expiry.receipt);
    harness.fleet.advance(QUEUE_FENCE_GRACE_MS);
    await fallback.alarm();
    const fallbackStorage = harness.fleet.cell('queue', fallbackName).storage;
    expect(fallbackStorage.data.has(`expired-run:${expiredRunId}`)).toBe(false);

    const runInstance = harness.fleet.cell('runs', expiredRunId).instance as {
      getLifecycleStatus(): Promise<unknown>;
    };
    const getLifecycleStatus = runInstance.getLifecycleStatus.bind(runInstance);
    let authorityRpcs = 0;
    runInstance.getLifecycleStatus = async () => {
      authorityRpcs++;
      return getLifecycleStatus();
    };
    fallbackStorage.resetOperationCounts();
    expiredRunStorage.resetOperationCounts();
    publicRpcs = 0;
    const rejected = await env.WORKFLOW_QUEUE.get(
      env.WORKFLOW_QUEUE.idFromName(fallbackName),
    ).enqueue(queueRequest('msg_queue_fallback', expiredRunId));
    const fallbackReport = {
      publicRpcs,
      internalAuthorityRpcs: authorityRpcs,
      queueStorage: storageCounts([fallbackStorage]),
      authorityStorage: storageCounts([expiredRunStorage]),
    };

    const deliveryRunId = 'wrun_queue_delivery_evidence';
    const deliveryRunStorage = harness.fleet.cell('runs', deliveryRunId).storage;
    deliveryRunStorage.data.set('run', { runId: deliveryRunId, status: 'running' });
    const deliveryRunInstance = harness.fleet.cell('runs', deliveryRunId).instance as {
      getLifecycleStatus(): Promise<unknown>;
    };
    const getDeliveryLifecycleStatus =
      deliveryRunInstance.getLifecycleStatus.bind(deliveryRunInstance);
    let deliveryAuthorityRpcs = 0;
    deliveryRunInstance.getLifecycleStatus = async () => {
      deliveryAuthorityRpcs++;
      return getDeliveryLifecycleStatus();
    };
    const deliveryName = 'q:lifecycle-delivery';
    const deliveryQueue = harness.fleet.namespace('queue').get({
      toString: () => deliveryName,
    }) as QueueDO;
    await env.WORKFLOW_QUEUE.get(env.WORKFLOW_QUEUE.idFromName(deliveryName)).enqueue({
      ...queueRequest('msg_queue_delivery', deliveryRunId),
      delaySeconds: 0,
    });
    const deliveryStorage = harness.fleet.cell('queue', deliveryName).storage;
    deliveryStorage.resetOperationCounts();
    deliveryRunStorage.resetOperationCounts();
    deliveryAuthorityRpcs = 0;
    callbackFetches = 0;
    await deliveryQueue.alarm();
    await harness.fleet.settle();
    const deliveryReport = {
      externalCallbackFetches: callbackFetches,
      internalAuthorityRpcs: deliveryAuthorityRpcs,
      queueStorage: storageCounts([deliveryStorage]),
      authorityStorage: storageCounts([deliveryRunStorage]),
    };
    console.log(
      `LIFECYCLE_COMPACTION_COST ${JSON.stringify({ steady: steadyReport, fallback: fallbackReport, delivery: deliveryReport })}`,
    );

    expect(steady).toMatchObject({ ok: true });
    expect(steadyReport.publicRpcs).toBe(1);
    expect(steadyReport).toEqual({
      publicRpcs: 1,
      internalAuthorityRpcs: 1,
      queueStorage: { ...emptyCounts(), get: 2, put: 4, transaction: 1 },
      authorityStorage: { ...emptyCounts(), getMany: 1, transaction: 1 },
    });
    expect(rejected).toMatchObject({ ok: false, code: 'RUN_EXPIRED' });
    expect(fallbackReport).toEqual({
      publicRpcs: 1,
      internalAuthorityRpcs: 1,
      queueStorage: { ...emptyCounts(), get: 1, transaction: 1 },
      authorityStorage: { ...emptyCounts(), getMany: 1, transaction: 1 },
    });
    expect(deliveryReport).toEqual({
      externalCallbackFetches: 1,
      internalAuthorityRpcs: 1,
      queueStorage: {
        ...emptyCounts(),
        getMany: 1,
        list: 10,
        putMany: 1,
        delete: 3,
        deleteMany: 1,
        transaction: 2,
      },
      authorityStorage: { ...emptyCounts(), getMany: 1, transaction: 1 },
    });
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
    const originalExpireRun = catalog.expireRun.bind(catalog);
    let internalCatalogExpireCalls = 0;
    catalog.expireRun = async (...args) => {
      internalCatalogExpireCalls++;
      return originalExpireRun(...args);
    };
    const catalogStorage = harness.fleet.cell('run-catalog', runCatalogShardName(runId)).storage;
    reset([catalogStorage]);
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
        runCatalogExpire: internalCatalogExpireCalls,
      },
      indexStorage: {
        runCatalog: storageCounts([catalogStorage]),
      },
      elapsedMs,
      finalPhase: status?.phase,
    };
    console.log(`INDEX_SCALABILITY_RETENTION ${JSON.stringify(report)}`);
    expect(internalCatalogExpireCalls).toBe(1);
    expect(report.indexStorage.runCatalog).toEqual({
      ...emptyCounts(),
      get: 1,
      put: 2,
      deleteMany: 1,
      transaction: 1,
    });
    expect(status?.phase).toBe('tombstoned');
  });
});
