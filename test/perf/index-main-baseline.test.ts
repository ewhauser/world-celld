import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { createRemoteEnv } from '../../src/remote/namespaces.js';
import { createStorage } from '../../src/storage.js';
import type { FakeStorageOperationCounts } from '../../src/testing/fake-cell.js';
import { startHarness, type Harness } from '../../src/testing/http-harness.js';

const SECRET = 'index-main-baseline-secret';
const WORKLOAD = 48;
const CONCURRENCY = 16;
const SYNTHETIC_TRANSACTION_MS = 2;

function percentile(values: number[], fraction: number): number {
  const ordered = values.toSorted((left, right) => left - right);
  return ordered[Math.min(ordered.length - 1, Math.floor(ordered.length * fraction))] ?? 0;
}

async function runPool(
  values: string[],
  operation: (value: string, index: number) => Promise<void>,
): Promise<number[]> {
  const latencies = Array.from<number>({ length: values.length });
  let next = 0;
  await Promise.all(
    Array.from({ length: CONCURRENCY }, async () => {
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

function resultMetric(elapsedMs: number, latencies: number[]) {
  return {
    elapsedMs,
    throughputPerSecond: (WORKLOAD * 1000) / elapsedMs,
    p50Ms: percentile(latencies, 0.5),
    p95Ms: percentile(latencies, 0.95),
  };
}

describe('recorded origin/main IndexDO baseline', () => {
  let harness: Harness;

  beforeAll(async () => {
    harness = await startHarness({ secret: SECRET, virtualClock: true });
  });

  afterAll(async () => {
    await harness.close();
  });

  it('records public RPC fanout, latency, and singleton storage work by caller', async () => {
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
      deploymentId: 'index-main-baseline',
    });
    const indexStorage = harness.fleet.cell('index', 'index').storage;
    const runId = 'wrun_main_baseline_fanout';
    const measure = async (operation: () => Promise<unknown>) => {
      publicRpcs = 0;
      paths.clear();
      indexStorage.resetOperationCounts();
      const startedAt = performance.now();
      await operation();
      return {
        publicRpcs,
        paths: Object.fromEntries(
          [...paths].toSorted(([left], [right]) => left.localeCompare(right)),
        ),
        elapsedMs: performance.now() - startedAt,
        indexStorage: { ...indexStorage.operationCounts },
      };
    };

    const runCreate = await measure(() =>
      storage.events.create(runId, {
        eventType: 'run_created',
        eventData: {
          deploymentId: 'index-main-baseline',
          workflowName: 'index-main-baseline',
          input: [],
        },
      }),
    );
    const runUpdate = await measure(() =>
      storage.events.create(runId, {
        eventType: 'attr_set',
        correlationId: 'baseline-attr',
        eventData: {
          changes: [{ key: 'baseline', value: 'updated' }],
          writer: { type: 'workflow' },
        },
      }),
    );
    const runListOne = await measure(() =>
      storage.runs.list({ workflowName: 'index-main-baseline', pagination: { limit: 20 } }),
    );
    const hookCreate = await measure(() =>
      storage.events.create(runId, {
        eventType: 'hook_created',
        correlationId: 'baseline-hook',
        eventData: { token: 'baseline-token' },
      }),
    );
    const hookGetByToken = await measure(() => storage.hooks.getByToken('baseline-token'));
    const hookGetById = await measure(() => storage.hooks.get('baseline-hook'));
    const hookResume = await measure(() =>
      storage.events.create(runId, {
        eventType: 'hook_received',
        correlationId: 'baseline-hook',
        eventData: { payload: 'ok' },
      }),
    );
    const hookDelete = await measure(() =>
      storage.events.create(runId, {
        eventType: 'hook_disposed',
        correlationId: 'baseline-hook',
      }),
    );
    for (let hook = 0; hook < 5; hook++) {
      await storage.events.create(runId, {
        eventType: 'hook_created',
        correlationId: `terminal-hook-${hook}`,
        eventData: { token: `terminal-token-${hook}` },
      });
    }
    const terminalFiveHooks = await measure(() =>
      storage.events.create(runId, {
        eventType: 'run_completed',
        eventData: { output: [] },
      }),
    );

    const report = {
      source: process.env.INDEX_BASELINE_REF ?? 'unknown',
      operations: {
        runCreate,
        runUpdate,
        runListOne,
        hookCreate,
        hookGetByToken,
        hookGetById,
        hookResume,
        hookDelete,
        terminalFiveHooks,
      },
    };
    console.log(`INDEX_MAIN_BASELINE_FANOUT ${JSON.stringify(report)}`);

    expect(runCreate.publicRpcs).toBe(3);
    expect(runUpdate.publicRpcs).toBe(3);
    expect(runListOne.publicRpcs).toBe(2);
    expect(hookCreate.publicRpcs).toBe(3);
    expect(hookGetByToken.publicRpcs).toBe(1);
    expect(hookGetById.publicRpcs).toBe(1);
    expect(hookResume.publicRpcs).toBe(1);
    expect(hookDelete.publicRpcs).toBe(2);
    expect(terminalFiveHooks.publicRpcs).toBe(4);
    expect(runCreate.indexStorage).toEqual({
      ...emptyCounts(),
      get: 2,
      put: 2,
      transaction: 2,
    });
    expect(runUpdate.indexStorage).toEqual(runCreate.indexStorage);
    expect(runListOne.indexStorage).toEqual({ ...emptyCounts(), list: 1 });
    expect(hookCreate.indexStorage).toEqual({
      ...emptyCounts(),
      getMany: 2,
      put: 1,
      putMany: 1,
      delete: 1,
      transaction: 2,
    });
    expect(hookGetByToken.indexStorage).toEqual({
      ...emptyCounts(),
      get: 1,
      getMany: 1,
    });
    expect(hookGetById.indexStorage).toEqual(hookGetByToken.indexStorage);
    expect(hookResume.indexStorage).toEqual(emptyCounts());
    expect(hookDelete.indexStorage).toEqual({
      ...emptyCounts(),
      getMany: 1,
      deleteMany: 1,
      transaction: 1,
    });
    expect(terminalFiveHooks.indexStorage).toEqual({
      ...emptyCounts(),
      get: 2,
      put: 3,
      transaction: 3,
    });
  });

  it('records global singleton contention independently of public RPC savings', async () => {
    const env = createRemoteEnv({ fleetUrl: harness.url, secret: SECRET });
    const storage = createStorage({
      env: { WORKFLOW_DB: env.WORKFLOW_DB, WORKFLOW_INDEX: env.WORKFLOW_INDEX },
      deploymentId: 'index-main-contention',
    });
    const indexStorage = harness.fleet.cell('index', 'index').storage;
    const originalTransaction = indexStorage.transaction.bind(indexStorage);
    indexStorage.transaction = async (callback) =>
      originalTransaction(async (transaction) => {
        await new Promise((resolve) => setTimeout(resolve, SYNTHETIC_TRANSACTION_MS));
        return callback(transaction);
      });
    const runIds = Array.from({ length: WORKLOAD }, (_, index) => `wrun_main_contention_${index}`);

    try {
      indexStorage.resetOperationCounts();
      const startedAt = performance.now();
      const latencies = await runPool(runIds, async (runId, index) => {
        await storage.events.create(runId, {
          eventType: 'run_created',
          eventData: {
            deploymentId: 'index-main-contention',
            workflowName: 'index-main-contention',
            input: [index],
          },
        });
      });
      const elapsedMs = performance.now() - startedAt;
      const singletonIndexStorage = { ...indexStorage.operationCounts };

      const stepStartedAt = performance.now();
      const stepLatencies = await runPool(runIds, async (runId, index) => {
        await storage.events.create(runId, {
          eventType: 'step_created',
          correlationId: `step-${index}`,
          eventData: { stepName: `step-${index}`, input: [] },
        });
      });
      const stepElapsedMs = performance.now() - stepStartedAt;

      console.log(
        `INDEX_MAIN_BASELINE_CONTENTION ${JSON.stringify({
          source: process.env.INDEX_BASELINE_REF ?? 'unknown',
          workload: WORKLOAD,
          concurrency: CONCURRENCY,
          syntheticTransactionMs: SYNTHETIC_TRANSACTION_MS,
          singletonRunCreate: resultMetric(elapsedMs, latencies),
          singletonIndexStorage,
          stepCreateNoIndex: resultMetric(stepElapsedMs, stepLatencies),
        })}`,
      );
      expect(singletonIndexStorage).toEqual({
        ...emptyCounts(),
        get: WORKLOAD * 2,
        put: WORKLOAD * 2,
        transaction: WORKLOAD * 2,
      });
    } finally {
      indexStorage.transaction = originalTransaction;
    }
  });

  it('records retention cleanup public and internal singleton work', async () => {
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
      deploymentId: 'index-main-retention',
      runRetentionMs: 1,
      queueShards: 1,
    });
    const runId = 'wrun_main_retention';
    await storage.events.create(runId, {
      eventType: 'run_created',
      eventData: {
        deploymentId: 'index-main-retention',
        workflowName: 'index-main-retention',
        input: [],
      },
    });
    await storage.events.create(runId, {
      eventType: 'run_completed',
      eventData: { output: [] },
    });

    const index = harness.fleet.namespace('index').get({ toString: () => 'index' }) as {
      expireRun(...args: unknown[]): Promise<unknown>;
    };
    const originalExpireRun = index.expireRun.bind(index);
    let internalExpireCalls = 0;
    index.expireRun = async (...args) => {
      internalExpireCalls++;
      return originalExpireRun(...args);
    };
    const indexStorage = harness.fleet.cell('index', 'index').storage;
    indexStorage.resetOperationCounts();
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
      index.expireRun = originalExpireRun;
    }
    const elapsedMs = performance.now() - startedAt;
    const status = await env.WORKFLOW_DB.get(env.WORKFLOW_DB.idFromName(runId)).getCleanupStatus();
    const report = {
      source: process.env.INDEX_BASELINE_REF ?? 'unknown',
      publicRpcs,
      paths: Object.fromEntries(
        [...paths].toSorted(([left], [right]) => left.localeCompare(right)),
      ),
      internalIndexRpcCalls: { expireRun: internalExpireCalls },
      indexStorage: { ...indexStorage.operationCounts },
      elapsedMs,
      finalPhase: status?.phase,
    };
    console.log(`INDEX_MAIN_BASELINE_RETENTION ${JSON.stringify(report)}`);
    expect(publicRpcs).toBe(9);
    expect(internalExpireCalls).toBe(1);
    expect(report.indexStorage).toEqual({
      ...emptyCounts(),
      getMany: 1,
      put: 3,
      deleteMany: 2,
      transaction: 3,
    });
    expect(status?.phase).toBe('tombstoned');
  });
});
