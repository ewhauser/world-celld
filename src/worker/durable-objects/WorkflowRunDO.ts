import { DurableObject } from '../do-base.js';
import type { Event, Hook, Step, WorkflowRun } from '@workflow/world';
import { isTerminalWorkflowRunStatus, slotToEventId, WorkflowRunSchema } from '@workflow/world';
import {
  applyEvent,
  finalizeEventPage,
  type ApplyEventOutcome,
  type ApplyEventRequest,
  EVENT_KEY_PREFIX,
  type EventStore,
  HOOK_CREATED_KEY_PREFIX,
  HOOK_KEY_PREFIX,
  listByCreationTime,
  listByPrefix,
  STEP_CREATED_KEY_PREFIX,
  STEP_KEY_PREFIX,
} from '../../apply-event.js';
import {
  CLEANUP_RECORD_KEY,
  type CleanupRecord,
  cleanupTombstone,
  type ExpireQueueRunResult,
  type ExpireRunIndexesRequest,
  type ExpireRunIndexesResult,
  type ExpireRunStreamsResult,
  type ExpireStreamResult,
  expiredRead,
  globalRunIndexKey,
  HOOK_MARKER_PREFIX,
  type HookIndexReference,
  hookMarkerKey,
  INDEX_MARKER_PREFIX,
  type RunReadOutcome,
  type RunTombstone,
  type ScheduleCleanupRequest,
  TOMBSTONE_KEY,
  workflowRunIndexKey,
} from '../../retention.js';

interface CellId {
  toString(): string;
}

interface CellNamespace<T> {
  idFromName(name: string): CellId;
  get(id: CellId): T;
}

interface IndexCleanupStub {
  expireRun(request: ExpireRunIndexesRequest): Promise<ExpireRunIndexesResult>;
}

interface StreamCleanupStub {
  expireRegistry(runId: string, expiredAt: number): Promise<ExpireRunStreamsResult>;
  finalizeRegistry(runId: string): Promise<void>;
  expireStream(runId: string, expiredAt: number): Promise<ExpireStreamResult>;
}

interface QueueCleanupStub {
  expireRun(runId: string, expiredAt: number): Promise<ExpireQueueRunResult>;
}

interface WorkflowRunDOEnv {
  WORKFLOW_INDEX?: CellNamespace<IndexCleanupStub>;
  WORKFLOW_STREAMS?: CellNamespace<StreamCleanupStub>;
  WORKFLOW_QUEUE?: CellNamespace<QueueCleanupStub>;
  /** Test seam; celld deployments use Date.now(). */
  clock?: () => number;
}

const PAYLOAD_DELETE_BATCH = 256;
const STREAM_DELETE_CONCURRENCY = 16;
const CLEANUP_RETRY_MAX_MS = 60 * 60 * 1000;
type TerminalRun = Extract<WorkflowRun, { status: 'completed' | 'failed' | 'cancelled' }>;

/**
 * Adapt DO storage (or a transaction) to the {@link EventStore} interface
 * shared with the test mocks. No casts: the methods line up structurally,
 * this just pins the subset we rely on.
 */
function storeFrom(storage: DurableObjectStorage | DurableObjectTransaction): EventStore {
  return {
    get: (key) => storage.get(key),
    put: (key, value) => storage.put(key, value),
    delete: (key) => storage.delete(key),
    list: (options) => storage.list(options),
  };
}

interface InflightClaim {
  messageId: string;
  claimedAt: number;
}

/**
 * Durable Object for managing a single workflow run's state.
 *
 * All event-sourced writes go through {@link applyEvent} inside a single
 * storage transaction: guard checks, the event append, and the entity
 * mutation are atomic even if the DO is evicted mid-operation.
 *
 * Instances named `claim:<queueName>:<idempotencyKey>` are used purely as
 * queue-message dedup claims (see claimInflight/releaseInflight).
 */
export class WorkflowRunDO extends DurableObject {
  private now(): number {
    const clock = (this.env as WorkflowRunDOEnv)?.clock;
    return typeof clock === 'function' ? clock() : Date.now();
  }

  private async expiredTombstone(
    storage: Pick<DurableObjectStorage, 'get'>,
    now = this.now(),
  ): Promise<RunTombstone | null> {
    const tombstone = await storage.get<RunTombstone>(TOMBSTONE_KEY);
    if (tombstone) return tombstone;
    const cleanup = await storage.get<CleanupRecord>(CLEANUP_RECORD_KEY);
    if (!cleanup || (cleanup.phase === 'retained' && cleanup.dueAt.getTime() > now)) return null;
    return cleanupTombstone(cleanup, cleanup.tombstonedAt ?? cleanup.dueAt);
  }

  private async read<T>(reader: () => Promise<T>): Promise<RunReadOutcome<T>> {
    const tombstone = await this.expiredTombstone(this.ctx.storage);
    if (tombstone) return expiredRead(tombstone);
    return { ok: true, value: await reader() };
  }

  /** Apply an event and capture all retention metadata in the same transaction. */
  async applyEvent(request: ApplyEventRequest): Promise<ApplyEventOutcome> {
    return await this.ctx.storage.transaction(async (txn) => {
      const now = new Date(this.now());
      const tombstone = await this.expiredTombstone(txn, now.getTime());
      if (tombstone) {
        return {
          ok: false,
          code: 'RUN_EXPIRED',
          message: `Workflow run "${request.runId}" expired at ${tombstone.expiredAt.toISOString()}`,
        };
      }

      let eventSequence = (await txn.get<number>('event_sequence')) ?? 0;
      const outcome = await applyEvent(storeFrom(txn), {
        ...request,
        nextEventId: () => slotToEventId(++eventSequence),
        now,
      });
      if (!outcome.ok) return outcome;

      await txn.put('event_sequence', eventSequence);
      if (outcome.hookToIndex) {
        const reference = {
          hookId: outcome.hookToIndex.hookId,
          token: outcome.hookToIndex.token,
        };
        await txn.put(hookMarkerKey(reference), reference);
      }
      for (const reference of outcome.releasedHooks) {
        await txn.put(hookMarkerKey(reference), reference);
      }

      const retentionMs = request.cleanup?.retentionMs ?? 0;
      if (
        retentionMs > 0 &&
        outcome.run &&
        isTerminalWorkflowRunStatus(outcome.run.status) &&
        !(await txn.get<CleanupRecord>(CLEANUP_RECORD_KEY))
      ) {
        const terminalRun = outcome.run as TerminalRun;
        const dueAt = new Date(terminalRun.completedAt.getTime() + retentionMs);
        const run = WorkflowRunSchema.parse({ ...terminalRun, expiredAt: dueAt }) as TerminalRun;
        const cleanup: CleanupRecord = {
          version: 1,
          runId: run.runId,
          workflowName: run.workflowName,
          createdAt: run.createdAt,
          completedAt: run.completedAt,
          terminalStatus: run.status,
          dueAt,
          queueShards: request.cleanup?.queueShards ?? 1,
          phase: 'retained',
          attempts: 0,
          deletedPayloadKeys: 0,
          deletedStreams: 0,
          deletedQueueMessages: 0,
        };
        await txn.put('run', run);
        await txn.put(CLEANUP_RECORD_KEY, cleanup);
        await txn.setAlarm(dueAt);
        outcome.run = run;
      }
      return finalizeEventPage(storeFrom(txn), outcome, request.params);
    });
  }

  async getRun(): Promise<RunReadOutcome<WorkflowRun | null>> {
    return this.read(async () => (await this.ctx.storage.get<WorkflowRun>('run')) ?? null);
  }

  async getStep(stepId: string): Promise<RunReadOutcome<Step | null>> {
    return this.read(
      async () => (await this.ctx.storage.get<Step>(`${STEP_KEY_PREFIX}${stepId}`)) ?? null,
    );
  }

  async listSteps(params?: {
    limit?: number;
    cursor?: string;
    sortOrder?: 'asc' | 'desc';
  }): Promise<RunReadOutcome<{ data: Step[]; cursor: string | null; hasMore: boolean }>> {
    return this.read(() =>
      listByCreationTime<Step>(
        storeFrom(this.ctx.storage),
        STEP_CREATED_KEY_PREFIX,
        STEP_KEY_PREFIX,
        {
          limit: params?.limit ?? 20,
          cursor: params?.cursor,
          sortOrder: params?.sortOrder ?? 'asc',
        },
      ),
    );
  }

  async getEvent(eventId: string): Promise<RunReadOutcome<Event | null>> {
    return this.read(
      async () => (await this.ctx.storage.get<Event>(`${EVENT_KEY_PREFIX}${eventId}`)) ?? null,
    );
  }

  async listEvents(params?: {
    limit?: number;
    cursor?: string;
    sortOrder?: 'asc' | 'desc';
  }): Promise<RunReadOutcome<{ data: Event[]; cursor: string | null; hasMore: boolean }>> {
    return this.read(() =>
      listByPrefix<Event>(
        storeFrom(this.ctx.storage),
        EVENT_KEY_PREFIX,
        {
          limit: params?.limit ?? 100,
          cursor: params?.cursor,
          sortOrder: params?.sortOrder ?? 'asc',
        },
        (event) => event.eventId,
      ),
    );
  }

  async listHooks(params?: {
    limit?: number;
    cursor?: string;
    sortOrder?: 'asc' | 'desc';
  }): Promise<RunReadOutcome<{ data: Hook[]; cursor: string | null; hasMore: boolean }>> {
    return this.read(() =>
      listByCreationTime<Hook>(
        storeFrom(this.ctx.storage),
        HOOK_CREATED_KEY_PREFIX,
        HOOK_KEY_PREFIX,
        {
          limit: params?.limit ?? 100,
          cursor: params?.cursor,
          sortOrder: params?.sortOrder ?? 'asc',
        },
      ),
    );
  }

  async getCleanupStatus(): Promise<CleanupRecord | null> {
    return (await this.ctx.storage.get<CleanupRecord>(CLEANUP_RECORD_KEY)) ?? null;
  }

  /** Explicitly schedule an existing terminal run, used for opt-in backfills. */
  async scheduleCleanup(request: ScheduleCleanupRequest): Promise<CleanupRecord | null> {
    if (request.retentionMs <= 0) return this.getCleanupStatus();
    return await this.ctx.storage.transaction(async (txn) => {
      const existing = await txn.get<CleanupRecord>(CLEANUP_RECORD_KEY);
      if (existing) return existing;
      if (await txn.get<RunTombstone>(TOMBSTONE_KEY)) return null;
      const current = await txn.get<WorkflowRun>('run');
      if (!current || !isTerminalWorkflowRunStatus(current.status)) return null;
      const terminalRun = current as TerminalRun;
      const dueAt = new Date(terminalRun.completedAt.getTime() + request.retentionMs);
      const run = WorkflowRunSchema.parse({ ...terminalRun, expiredAt: dueAt }) as TerminalRun;
      const cleanup: CleanupRecord = {
        version: 1,
        runId: run.runId,
        workflowName: run.workflowName,
        createdAt: run.createdAt,
        completedAt: run.completedAt,
        terminalStatus: run.status,
        dueAt,
        queueShards: request.queueShards,
        phase: 'retained',
        attempts: 0,
        deletedPayloadKeys: 0,
        deletedStreams: 0,
        deletedQueueMessages: 0,
      };
      await txn.put('run', run);
      await txn.put(CLEANUP_RECORD_KEY, cleanup);
      await txn.setAlarm(dueAt.getTime() <= this.now() ? this.now() + 1 : dueAt);
      return cleanup;
    });
  }

  async cleanupNow(request: ScheduleCleanupRequest): Promise<CleanupRecord | null> {
    if (!(await this.ctx.storage.get<CleanupRecord>(CLEANUP_RECORD_KEY))) {
      await this.scheduleCleanup({ ...request, retentionMs: Math.max(1, request.retentionMs) });
    }
    await this.ctx.storage.transaction(async (txn) => {
      const cleanup = await txn.get<CleanupRecord>(CLEANUP_RECORD_KEY);
      if (!cleanup || cleanup.phase === 'tombstoned') return;
      const dueAt = new Date(this.now());
      const run = await txn.get<WorkflowRun>('run');
      if (run) await txn.put('run', WorkflowRunSchema.parse({ ...run, expiredAt: dueAt }));
      await txn.put(CLEANUP_RECORD_KEY, { ...cleanup, dueAt });
      await txn.setAlarm(this.now() + 1);
    });
    try {
      await this.executeCleanup();
    } catch (error) {
      await this.recordCleanupFailure(error);
    }
    return this.getCleanupStatus();
  }

  async rearmCleanup(): Promise<CleanupRecord | null> {
    const cleanup = await this.ctx.storage.get<CleanupRecord>(CLEANUP_RECORD_KEY);
    if (!cleanup) return null;
    if (cleanup.phase === 'tombstoned') {
      await this.ctx.storage.deleteAlarm();
    } else {
      await this.ctx.storage.setAlarm(Math.max(this.now() + 1, cleanup.dueAt.getTime()));
    }
    return cleanup;
  }

  async alarm(): Promise<void> {
    try {
      await this.executeCleanup();
    } catch (error) {
      await this.recordCleanupFailure(error);
    }
  }

  private namespace<T>(name: keyof WorkflowRunDOEnv): CellNamespace<T> {
    const namespace = (this.env as WorkflowRunDOEnv)?.[name];
    if (!namespace || typeof namespace === 'function') {
      throw new Error(`world-celld retention missing binding ${name}`);
    }
    return namespace as CellNamespace<T>;
  }

  private async setCleanupPhase(
    expected: CleanupRecord['phase'],
    next: CleanupRecord['phase'],
    updates: Partial<CleanupRecord> = {},
  ): Promise<void> {
    await this.ctx.storage.transaction(async (txn) => {
      const cleanup = await txn.get<CleanupRecord>(CLEANUP_RECORD_KEY);
      if (!cleanup || cleanup.phase !== expected) return;
      await txn.put(CLEANUP_RECORD_KEY, {
        ...cleanup,
        ...updates,
        phase: next,
        attempts: 0,
        lastError: undefined,
      });
    });
  }

  private async executeCleanup(): Promise<void> {
    for (;;) {
      const cleanup = await this.ctx.storage.get<CleanupRecord>(CLEANUP_RECORD_KEY);
      if (!cleanup) {
        await this.ctx.storage.deleteAlarm();
        return;
      }
      if (cleanup.phase === 'tombstoned') {
        await this.ctx.storage.deleteAlarm();
        return;
      }
      if (cleanup.phase === 'retained') {
        if (cleanup.dueAt.getTime() > this.now()) {
          await this.ctx.storage.setAlarm(cleanup.dueAt);
          return;
        }
        await this.setCleanupPhase('retained', 'index');
        continue;
      }

      if (cleanup.phase === 'index') {
        // Older deployments wrote correlation-index keys plus per-run markers.
        // New events no longer create either, but keep consuming old markers so
        // retention still removes already-persisted global keys.
        const [indexMarkers, hookMarkers] = await Promise.all([
          this.ctx.storage.list<string>({ prefix: INDEX_MARKER_PREFIX }),
          this.ctx.storage.list<HookIndexReference>({ prefix: HOOK_MARKER_PREFIX }),
        ]);
        const indexNamespace = this.namespace<IndexCleanupStub>('WORKFLOW_INDEX');
        const index = indexNamespace.get(indexNamespace.idFromName('index'));
        await index.expireRun({
          runId: cleanup.runId,
          keys: [
            workflowRunIndexKey(cleanup),
            globalRunIndexKey(cleanup),
            ...indexMarkers.values(),
          ],
          hooks: Array.from(hookMarkers.values()),
          expiredAt: cleanup.dueAt.getTime(),
        });
        await this.setCleanupPhase('index', 'streams');
        continue;
      }

      if (cleanup.phase === 'streams') {
        const streamsNamespace = this.namespace<StreamCleanupStub>('WORKFLOW_STREAMS');
        const registry = streamsNamespace.get(
          streamsNamespace.idFromName(`run-streams:${cleanup.runId}`),
        );
        const { streams } = await registry.expireRegistry(cleanup.runId, cleanup.dueAt.getTime());
        let deletedStreams = streams.length === 0 ? cleanup.deletedStreams : 0;
        for (let offset = 0; offset < streams.length; offset += STREAM_DELETE_CONCURRENCY) {
          const batch = streams.slice(offset, offset + STREAM_DELETE_CONCURRENCY);
          const results = await Promise.all(
            batch.map((name) => {
              const stream = streamsNamespace.get(streamsNamespace.idFromName(`stream:${name}`));
              return stream.expireStream(cleanup.runId, cleanup.dueAt.getTime());
            }),
          );
          deletedStreams += results.filter((result) => result.deleted).length;
        }
        await this.ctx.storage.transaction(async (txn) => {
          const current = await txn.get<CleanupRecord>(CLEANUP_RECORD_KEY);
          if (current?.phase === 'streams') {
            await txn.put(CLEANUP_RECORD_KEY, { ...current, deletedStreams });
          }
        });
        await registry.finalizeRegistry(cleanup.runId);
        await this.setCleanupPhase('streams', 'queues');
        continue;
      }

      if (cleanup.phase === 'queues') {
        const queues = this.namespace<QueueCleanupStub>('WORKFLOW_QUEUE');
        let deletedQueueMessages = 0;
        for (let shard = 0; shard < cleanup.queueShards; shard++) {
          const queue = queues.get(queues.idFromName(`q:${shard}`));
          const result = await queue.expireRun(cleanup.runId, cleanup.dueAt.getTime());
          deletedQueueMessages += result.deleted;
        }
        await this.setCleanupPhase('queues', 'payload', { deletedQueueMessages });
        continue;
      }

      if (cleanup.phase === 'payload') {
        const finished = await this.deletePayloadBatch();
        if (!finished) return;
      }
    }
  }

  private async deletePayloadBatch(): Promise<boolean> {
    return await this.ctx.storage.transaction(async (txn) => {
      const cleanup = await txn.get<CleanupRecord>(CLEANUP_RECORD_KEY);
      if (!cleanup || cleanup.phase !== 'payload') return true;
      const entries = await txn.list({ limit: PAYLOAD_DELETE_BATCH + 3 });
      const keys = Array.from(entries.keys())
        .filter((key) => key !== CLEANUP_RECORD_KEY && key !== TOMBSTONE_KEY)
        .slice(0, PAYLOAD_DELETE_BATCH);
      const now = new Date(this.now());
      await txn.put(TOMBSTONE_KEY, cleanupTombstone(cleanup, now));
      for (const key of keys) await txn.delete(key);

      if (keys.length > 0) {
        await txn.put(CLEANUP_RECORD_KEY, {
          ...cleanup,
          deletedPayloadKeys: cleanup.deletedPayloadKeys + keys.length,
          lastError: undefined,
        });
        await txn.setAlarm(this.now() + 1);
        return false;
      }

      await txn.put(CLEANUP_RECORD_KEY, {
        ...cleanup,
        phase: 'tombstoned',
        attempts: 0,
        lastError: undefined,
        tombstonedAt: now,
      });
      await txn.deleteAlarm();
      return true;
    });
  }

  private async recordCleanupFailure(error: unknown): Promise<void> {
    await this.ctx.storage.transaction(async (txn) => {
      const cleanup = await txn.get<CleanupRecord>(CLEANUP_RECORD_KEY);
      if (!cleanup || cleanup.phase === 'tombstoned') return;
      const attempts = cleanup.attempts + 1;
      const delay = Math.min(CLEANUP_RETRY_MAX_MS, 1000 * 2 ** Math.min(attempts - 1, 12));
      await txn.put(CLEANUP_RECORD_KEY, {
        ...cleanup,
        attempts,
        lastError: error instanceof Error ? error.message : String(error),
        lastAttemptAt: new Date(this.now()),
      });
      await txn.setAlarm(this.now() + delay);
    });
  }

  async claimInflight(params: {
    messageId: string;
    staleMs: number;
  }): Promise<{ claimed: boolean }> {
    return await this.ctx.storage.transaction(async (txn) => {
      const existing = await txn.get<InflightClaim>('claim');
      const now = this.now();
      if (
        existing &&
        existing.messageId !== params.messageId &&
        now - existing.claimedAt < params.staleMs
      ) {
        return { claimed: false };
      }
      await txn.put<InflightClaim>('claim', { messageId: params.messageId, claimedAt: now });
      return { claimed: true };
    });
  }

  async releaseInflight(): Promise<void> {
    await this.ctx.storage.delete('claim');
  }
}
