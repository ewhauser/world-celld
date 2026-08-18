import type { TerminalWorkflowRunStatus, WorkflowRun } from '@workflow/world';

export const CLEANUP_RECORD_KEY = 'retention:cleanup';
export const TOMBSTONE_KEY = 'retention:tombstone';
/** Legacy correlation-index markers retained for upgrade cleanup compatibility. */
export const INDEX_MARKER_PREFIX = 'retention:index:';
export const HOOK_MARKER_PREFIX = 'retention:hook:';

export type CleanupPhase = 'retained' | 'index' | 'streams' | 'queues' | 'payload' | 'tombstoned';

export interface CleanupRecord {
  version: 1;
  runId: string;
  workflowName: string;
  createdAt: Date;
  completedAt: Date;
  terminalStatus: TerminalWorkflowRunStatus;
  dueAt: Date;
  queueShards: number;
  phase: CleanupPhase;
  attempts: number;
  lastError?: string;
  lastAttemptAt?: Date;
  tombstonedAt?: Date;
  deletedPayloadKeys: number;
  deletedStreams: number;
  deletedQueueMessages: number;
}

export interface RunTombstone {
  version: 1;
  runId: string;
  terminalStatus: TerminalWorkflowRunStatus;
  completedAt: Date;
  expiredAt: Date;
  tombstonedAt: Date;
}

export type RunReadOutcome<T> =
  | { ok: true; value: T }
  | { ok: false; code: 'RUN_EXPIRED'; message: string; tombstone: RunTombstone };

export interface HookIndexReference {
  hookId: string;
  token: string;
}

export interface ExpireRunIndexesRequest {
  runId: string;
  keys: string[];
  hooks: HookIndexReference[];
  expiredAt: number;
}

export interface ExpireRunIndexesResult {
  deleted: number;
}

export interface ExpireRunStreamsResult {
  streams: string[];
}

export interface ExpireStreamResult {
  deleted: boolean;
  chunks: number;
}

export interface ExpireQueueRunResult {
  deleted: number;
}

export interface ScheduleCleanupRequest {
  retentionMs: number;
  queueShards: number;
}

export function sortableTimestamp(date: Date): string {
  return date.getTime().toString().padStart(13, '0');
}

export function workflowRunIndexKey(
  run: Pick<WorkflowRun, 'workflowName' | 'createdAt' | 'runId'>,
) {
  return `run:${run.workflowName}:${sortableTimestamp(run.createdAt)}:${run.runId}`;
}

export function globalRunIndexKey(run: Pick<WorkflowRun, 'createdAt' | 'runId'>) {
  return `runall:${sortableTimestamp(run.createdAt)}:${run.runId}`;
}

export function hookMarkerKey(reference: HookIndexReference): string {
  return `${HOOK_MARKER_PREFIX}${encodeURIComponent(reference.hookId)}:${encodeURIComponent(reference.token)}`;
}

export function cleanupTombstone(record: CleanupRecord, now: Date): RunTombstone {
  return {
    version: 1,
    runId: record.runId,
    terminalStatus: record.terminalStatus,
    completedAt: record.completedAt,
    expiredAt: record.dueAt,
    tombstonedAt: now,
  };
}

export function expiredRead<T>(tombstone: RunTombstone): RunReadOutcome<T> {
  return {
    ok: false,
    code: 'RUN_EXPIRED',
    message: `Workflow run "${tombstone.runId}" expired at ${tombstone.expiredAt.toISOString()}`,
    tombstone,
  };
}
