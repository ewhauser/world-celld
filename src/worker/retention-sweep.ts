import {
  createWorkflowIndex,
  type CellNamespaceLike,
  type HookIdShardStub,
  type HookTokenShardStub,
  runCatalogShardName,
  type RunCatalogShardStub,
} from '../indexes.js';
import {
  sortableTimestamp,
  type EnforceRetentionRequest,
  type EnforceRetentionResult,
} from '../retention.js';
import { isRecord, MAX_QUEUE_SHARDS, strictIntegerSetting } from '../validation.js';

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
  invalid: number;
}

function requireNamespace<T>(
  name: keyof Pick<
    RetentionSweepEnv,
    'WORKFLOW_DB' | 'WORKFLOW_RUN_CATALOG' | 'WORKFLOW_HOOK_TOKENS' | 'WORKFLOW_HOOK_IDS'
  >,
  namespace: RetentionNamespace<T> | undefined,
): RetentionNamespace<T> {
  if (
    !namespace ||
    typeof namespace.idFromName !== 'function' ||
    typeof namespace.get !== 'function'
  ) {
    throw new Error(`world-celld retention sweep missing binding ${name}`);
  }
  return namespace;
}

function catalogCandidate(name: string, value: string): { runId: string; invalid: boolean } {
  const keyMatch = /^runall:\d{13}:(.+)$/.exec(name);
  if (!keyMatch?.[1]) {
    throw new Error('world-celld: run catalog entry has an invalid global key');
  }
  const runId = keyMatch[1];
  try {
    const parsed: unknown = JSON.parse(value);
    return { runId, invalid: !isRecord(parsed) || parsed.runId !== runId };
  } catch {
    return { runId, invalid: true };
  }
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
  const retentionMs = strictIntegerSetting(
    'world-celld: WORKFLOW_RETENTION_MS',
    env.WORKFLOW_RETENTION_MS,
    0,
    0,
  );
  if (retentionMs === 0 || scheduledTime < retentionMs) {
    return {
      disabled: retentionMs === 0,
      cutoff: retentionMs === 0 ? null : scheduledTime - retentionMs,
      scanned: 0,
      scheduled: 0,
      expired: 0,
      missing: 0,
      notDue: 0,
      invalid: 0,
    };
  }
  const batchSize = strictIntegerSetting(
    'world-celld: WORKFLOW_RETENTION_BATCH_SIZE',
    env.WORKFLOW_RETENTION_BATCH_SIZE,
    DEFAULT_RETENTION_SWEEP_BATCH_SIZE,
    1,
    MAX_RETENTION_SWEEP_BATCH_SIZE,
  );
  const queueShards = strictIntegerSetting(
    'world-celld: WORKFLOW_RETENTION_QUEUE_SHARDS',
    env.WORKFLOW_RETENTION_QUEUE_SHARDS,
    1,
    1,
    MAX_QUEUE_SHARDS,
  );
  const cutoff = scheduledTime - retentionMs;
  const runNamespace = requireNamespace('WORKFLOW_DB', env.WORKFLOW_DB);
  const catalogNamespace = requireNamespace('WORKFLOW_RUN_CATALOG', env.WORKFLOW_RUN_CATALOG);
  const index = createWorkflowIndex({
    runCatalog: catalogNamespace,
    hookTokens: requireNamespace('WORKFLOW_HOOK_TOKENS', env.WORKFLOW_HOOK_TOKENS),
    hookIds: requireNamespace('WORKFLOW_HOOK_IDS', env.WORKFLOW_HOOK_IDS),
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
    invalid: 0,
  };
  const failures: unknown[] = [];
  for (let offset = 0; offset < page.keys.length; offset += RETENTION_SWEEP_CONCURRENCY) {
    const candidates = page.keys.slice(offset, offset + RETENTION_SWEEP_CONCURRENCY);
    const settled = await Promise.allSettled(
      candidates.map(async (entry) => {
        const { runId, invalid } = catalogCandidate(entry.name, entry.value);
        const catalog = catalogNamespace.get(
          catalogNamespace.idFromName(runCatalogShardName(runId)),
        );
        if (invalid) {
          await catalog.deleteStaleGlobalRun(runId, entry.name, entry.value);
          return { state: 'invalid' } as const;
        }
        const run = runNamespace.get(runNamespace.idFromName(runId));
        const outcome = await run.enforceRetention({ retentionMs, queueShards, scheduledTime });
        if (outcome.state === 'missing' || outcome.state === 'not-due') {
          await catalog.deleteStaleGlobalRun(runId, entry.name, entry.value);
        }
        return outcome;
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
