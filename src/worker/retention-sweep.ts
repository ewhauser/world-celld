import {
  createWorkflowIndex,
  type CellNamespaceLike,
  type HookIdShardStub,
  type HookTokenShardStub,
  type RunCatalogShardStub,
} from '../indexes.js';
import {
  sortableTimestamp,
  type EnforceRetentionRequest,
  type EnforceRetentionResult,
} from '../retention.js';

export const DEFAULT_RETENTION_SWEEP_BATCH_SIZE = 128;
export const MAX_RETENTION_SWEEP_BATCH_SIZE = 1000;
const RETENTION_SWEEP_CONCURRENCY = 8;

interface RetentionRunStub {
  enforceRetention(request: EnforceRetentionRequest): Promise<EnforceRetentionResult>;
}

type RetentionNamespace<T> = CellNamespaceLike<T>;

export interface RetentionSweepEnv {
  WORKFLOW_DB: RetentionNamespace<RetentionRunStub>;
  WORKFLOW_RUN_CATALOG: RetentionNamespace<RunCatalogShardStub>;
  WORKFLOW_HOOK_TOKENS: RetentionNamespace<HookTokenShardStub>;
  WORKFLOW_HOOK_IDS: RetentionNamespace<HookIdShardStub>;
  /** Fleet-wide maximum age from run creation. Zero disables the sweep. */
  WORKFLOW_RETENTION_MS?: string | number;
  /** Maximum catalog entries admitted by one cron occurrence. */
  WORKFLOW_RETENTION_BATCH_SIZE?: string | number;
  /** Queue-shard fallback for runs created before placement was persisted. */
  WORKFLOW_RETENTION_QUEUE_SHARDS?: string | number;
}

export interface RetentionSweepResult {
  disabled: boolean;
  cutoff: number | null;
  scanned: number;
  scheduled: number;
  expired: number;
  missing: number;
  notDue: number;
}

function integerSetting(
  name: string,
  raw: string | number | undefined,
  fallback: number,
  minimum: number,
  maximum = Number.MAX_SAFE_INTEGER,
): number {
  const value = raw === undefined || raw === '' ? fallback : Number(raw);
  if (!Number.isSafeInteger(value) || value < minimum || value > maximum) {
    throw new Error(`world-celld: ${name} must be an integer between ${minimum} and ${maximum}`);
  }
  return value;
}

function runIdFromCatalogValue(value: string): string {
  const parsed = JSON.parse(value) as { runId?: unknown };
  if (typeof parsed.runId !== 'string' || parsed.runId.length === 0) {
    throw new Error('world-celld: run catalog entry has no runId');
  }
  return parsed.runId;
}

/**
 * Discover and fence one bounded page of workflows older than the configured
 * maximum age. RunDO alarms finish the persisted, idempotent deletion phases.
 */
export async function runRetentionSweep(
  scheduledTime: number,
  env: RetentionSweepEnv,
): Promise<RetentionSweepResult> {
  if (!Number.isSafeInteger(scheduledTime) || scheduledTime < 0) {
    throw new Error('world-celld: scheduledTime must be a non-negative safe integer');
  }
  const retentionMs = integerSetting('WORKFLOW_RETENTION_MS', env.WORKFLOW_RETENTION_MS, 0, 0);
  if (retentionMs === 0 || scheduledTime < retentionMs) {
    return {
      disabled: retentionMs === 0,
      cutoff: retentionMs === 0 ? null : scheduledTime - retentionMs,
      scanned: 0,
      scheduled: 0,
      expired: 0,
      missing: 0,
      notDue: 0,
    };
  }
  const batchSize = integerSetting(
    'WORKFLOW_RETENTION_BATCH_SIZE',
    env.WORKFLOW_RETENTION_BATCH_SIZE,
    DEFAULT_RETENTION_SWEEP_BATCH_SIZE,
    1,
    MAX_RETENTION_SWEEP_BATCH_SIZE,
  );
  const queueShards = integerSetting(
    'WORKFLOW_RETENTION_QUEUE_SHARDS',
    env.WORKFLOW_RETENTION_QUEUE_SHARDS,
    1,
    1,
  );
  const cutoff = scheduledTime - retentionMs;
  const index = createWorkflowIndex({
    runCatalog: env.WORKFLOW_RUN_CATALOG,
    hookTokens: env.WORKFLOW_HOOK_TOKENS,
    hookIds: env.WORKFLOW_HOOK_IDS,
  });
  const page = await index.listRuns({
    prefix: 'runall:',
    end: `runall:${sortableTimestamp(new Date(cutoff + 1))}:`,
    limit: batchSize,
  });

  const result: RetentionSweepResult = {
    disabled: false,
    cutoff,
    scanned: page.keys.length,
    scheduled: 0,
    expired: 0,
    missing: 0,
    notDue: 0,
  };
  const failures: unknown[] = [];
  for (let offset = 0; offset < page.keys.length; offset += RETENTION_SWEEP_CONCURRENCY) {
    const candidates = page.keys.slice(offset, offset + RETENTION_SWEEP_CONCURRENCY);
    const settled = await Promise.allSettled(
      candidates.map(async (entry) => {
        const runId = runIdFromCatalogValue(entry.value);
        const run = env.WORKFLOW_DB.get(env.WORKFLOW_DB.idFromName(runId));
        return await run.enforceRetention({ retentionMs, queueShards, scheduledTime });
      }),
    );
    for (const outcome of settled) {
      if (outcome.status === 'rejected') {
        failures.push(outcome.reason);
      } else {
        result[outcome.value.state === 'not-due' ? 'notDue' : outcome.value.state]++;
      }
    }
  }
  if (failures.length > 0) {
    throw new AggregateError(
      failures,
      `world-celld retention sweep failed for ${failures.length} run(s)`,
    );
  }
  return result;
}
