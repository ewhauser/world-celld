import type { TerminalWorkflowRunStatus, WorkflowRun, WorkflowRunStatus } from '@workflow/world';

export const CLEANUP_RECORD_KEY = 'retention:cleanup';
export const TOMBSTONE_KEY = 'retention:tombstone';
export const HOOK_MARKER_PREFIX = 'retention:hook:';
export const TERMINAL_CLEANUP_KEY = 'terminal:cleanup';

export type CleanupPhase = 'retained' | 'index' | 'streams' | 'queues' | 'payload' | 'tombstoned';

export interface CleanupRecord {
  version: 1;
  runId: string;
  workflowName: string;
  createdAt: Date;
  /** Last status observed when cleanup was scheduled. */
  status?: WorkflowRunStatus;
  /** Present when cleanup was scheduled for a terminal run. */
  completedAt?: Date;
  /** Retained for persisted terminal-retention records created before max-age cleanup. */
  terminalStatus?: TerminalWorkflowRunStatus;
  reason?: 'terminal-retention' | 'maximum-age' | 'manual';
  dueAt: Date;
  queueShards: number;
  phase: CleanupPhase;
  /** Optimistic-concurrency token for alarm/RPC work that awaits another cell. */
  generation: number;
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
  /** Last status observed before the run expired. */
  status?: WorkflowRunStatus;
  completedAt?: Date;
  terminalStatus?: TerminalWorkflowRunStatus;
  expiredAt: Date;
  tombstonedAt: Date;
  /** Final cleanup accounting retained inside the single authoritative record. */
  cleanup: CleanupRecord;
}

export type RunLifecycleStatus = 'active' | 'terminal' | 'expired' | 'missing';

export type RunReadOutcome<T> =
  | { ok: true; value: T }
  | { ok: false; code: 'RUN_EXPIRED'; message: string; tombstone: RunTombstone };

export interface HookIndexReference {
  hookId: string;
  token: string;
}

export interface TerminalCleanupRecord {
  version: 1;
  runId: string;
  phase: 'hooks' | 'markers' | 'waits';
  generation: number;
  attempts: number;
  lastError?: string;
  lastAttemptAt?: Date;
}

export interface ReleaseHookIndexesRequest {
  runId: string;
  hooks: HookIndexReference[];
}

export interface ReleaseHookIndexesResult {
  deleted: number;
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

export interface FinalizeRunStreamsResult {
  /** Cumulative registry entries removed for this run. */
  deleted: number;
  done: boolean;
}

export interface ExpireStreamResult {
  /** True only when this call installed the stream's expiration fence. */
  deleted: boolean;
  /** Chunk rows removed by this bounded call. */
  chunks: number;
  /** Payload bytes represented by the removed chunk rows. */
  bytes: number;
  done: boolean;
}

export interface QueueExpiryReceipt {
  expiredAt: number;
  deleted: number;
}

export type ExpireQueueRunResult = {
  /** Cumulative messages removed for this run in this queue shard. */
  deleted: number;
} & (
  | { done: false }
  | {
      done: true;
      /** Durable receipt which the cleanup coordinator must acknowledge. */
      receipt: QueueExpiryReceipt;
    }
);

export interface AcknowledgeQueueExpiryResult {
  acknowledged: boolean;
}

export interface ScheduleCleanupRequest {
  retentionMs: number;
  queueShards: number;
}

export interface EnforceRetentionRequest extends ScheduleCleanupRequest {
  /** Cron occurrence being enforced. A run is eligible only at/before this time. */
  scheduledTime: number;
}

export type EnforceRetentionResult =
  | { state: 'missing' | 'not-due'; cleanup: null }
  | { state: 'scheduled' | 'expired'; cleanup: CleanupRecord };

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
  const cleanup: CleanupRecord = {
    ...record,
    phase: 'tombstoned',
    tombstonedAt: now,
  };
  return {
    version: 1,
    runId: record.runId,
    status: record.status ?? record.terminalStatus,
    terminalStatus: record.terminalStatus,
    completedAt: record.completedAt,
    expiredAt: record.dueAt,
    tombstonedAt: now,
    cleanup,
  };
}

export function cleanupFromTombstone(tombstone: RunTombstone): CleanupRecord {
  return tombstone.cleanup;
}

export function expiredRead<T>(tombstone: RunTombstone): RunReadOutcome<T> {
  return {
    ok: false,
    code: 'RUN_EXPIRED',
    message: `Workflow run "${tombstone.runId}" expired at ${tombstone.expiredAt.toISOString()}`,
    tombstone,
  };
}
