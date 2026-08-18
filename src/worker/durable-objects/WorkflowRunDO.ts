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
  WAIT_KEY_PREFIX,
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
  type FinalizeRunStreamsResult,
  expiredRead,
  globalRunIndexKey,
  HOOK_MARKER_PREFIX,
  type HookIndexReference,
  hookMarkerKey,
  type ReleaseHookIndexesRequest,
  type ReleaseHookIndexesResult,
  type RunReadOutcome,
  type RunTombstone,
  type ScheduleCleanupRequest,
  TERMINAL_CLEANUP_KEY,
  type TerminalCleanupRecord,
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
  releaseHookIndexes(request: ReleaseHookIndexesRequest): Promise<ReleaseHookIndexesResult>;
}

interface StreamCleanupStub {
  expireRegistry(
    runId: string,
    expiredAt: number,
    options?: { limit?: number },
  ): Promise<ExpireRunStreamsResult>;
  finalizeRegistry(runId: string, streams: string[]): Promise<FinalizeRunStreamsResult>;
  expireStream(
    runId: string,
    expiredAt: number,
    options?: { limit?: number; byteLimit?: number },
  ): Promise<ExpireStreamResult>;
}

interface QueueCleanupStub {
  expireRun(
    runId: string,
    expiredAt: number,
    options?: { limit?: number },
  ): Promise<ExpireQueueRunResult>;
}

interface WorkflowRunDOEnv {
  WORKFLOW_INDEX?: CellNamespace<IndexCleanupStub>;
  WORKFLOW_STREAMS?: CellNamespace<StreamCleanupStub>;
  WORKFLOW_QUEUE?: CellNamespace<QueueCleanupStub>;
  /** Test seam; celld deployments use Date.now(). */
  clock?: () => number;
}

const STORAGE_BATCH_SIZE = 128;
const PAYLOAD_DELETE_BATCH = STORAGE_BATCH_SIZE;
const HOOK_MARKER_PAGE_SIZE = 64;
const TERMINAL_HOOK_PAGE_SIZE = 64;
const TERMINAL_WAIT_PAGE_SIZE = STORAGE_BATCH_SIZE;
const STREAMS_PER_CLEANUP_PAGE = 16;
const STREAM_CHUNKS_PER_CLEANUP_PAGE = STORAGE_BATCH_SIZE;
const STREAM_BYTES_PER_CLEANUP_PAGE = 16 * 1024 * 1024;
const QUEUE_REFERENCES_PER_CLEANUP_PAGE = 64;
const QUEUE_SHARDS_PER_CLEANUP_PAGE = 8;
const CLEANUP_RETRY_MAX_MS = 60 * 60 * 1000;
const CLEANUP_PROGRESS_KEY = 'retention:progress';
type TerminalRun = Extract<WorkflowRun, { status: 'completed' | 'failed' | 'cancelled' }>;

interface CleanupProgress {
  queueShard: number;
  queueShardDeleted: number;
}

/**
 * Adapt DO storage (or a transaction) to the {@link EventStore} interface
 * shared with the test mocks. No casts: the methods line up structurally,
 * this just pins the subset we rely on.
 */
function storeFrom(storage: DurableObjectStorage | DurableObjectTransaction): EventStore {
  return {
    get: (key) => storage.get(key),
    getMany: (keys) => storage.get(keys),
    put: (key, value) => storage.put(key, value),
    delete: (key) => storage.delete(key),
    deleteMany: (keys) => storage.delete(keys),
    list: (options) => storage.list(options),
    deferTerminalCleanup: true,
  };
}

function hookCreationIndexKey(hook: Hook): string {
  return `${HOOK_CREATED_KEY_PREFIX}${hook.createdAt.toISOString()}:${hook.hookId}`;
}

async function putStorageEntries(
  storage: DurableObjectTransaction,
  entries: Iterable<readonly [string, unknown]>,
): Promise<void> {
  const all = Array.from(entries);
  for (let offset = 0; offset < all.length; offset += STORAGE_BATCH_SIZE) {
    await storage.put(Object.fromEntries(all.slice(offset, offset + STORAGE_BATCH_SIZE)));
  }
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

  private tombstoneFrom(
    values: Map<string, RunTombstone | CleanupRecord>,
    now = this.now(),
  ): RunTombstone | null {
    const tombstone = values.get(TOMBSTONE_KEY) as RunTombstone | undefined;
    if (tombstone) return tombstone;
    const cleanup = values.get(CLEANUP_RECORD_KEY) as CleanupRecord | undefined;
    if (!cleanup || (cleanup.phase === 'retained' && cleanup.dueAt.getTime() > now)) return null;
    return cleanupTombstone(cleanup, cleanup.tombstonedAt ?? cleanup.dueAt);
  }

  private async retentionState(
    storage: DurableObjectStorage | DurableObjectTransaction,
    now = this.now(),
  ): Promise<{ cleanup?: CleanupRecord; tombstone: RunTombstone | null }> {
    const values = await storage.get<RunTombstone | CleanupRecord>([
      TOMBSTONE_KEY,
      CLEANUP_RECORD_KEY,
    ]);
    return {
      cleanup: values.get(CLEANUP_RECORD_KEY) as CleanupRecord | undefined,
      tombstone: this.tombstoneFrom(values, now),
    };
  }

  private async read<T>(
    reader: (storage: DurableObjectTransaction) => Promise<T>,
  ): Promise<RunReadOutcome<T>> {
    return await this.ctx.storage.transaction(async (txn) => {
      const { tombstone } = await this.retentionState(txn);
      if (tombstone) return expiredRead(tombstone);
      return { ok: true, value: await reader(txn) };
    });
  }

  private async readKey<T>(key: string): Promise<RunReadOutcome<T | null>> {
    return await this.ctx.storage.transaction(async (txn) => {
      const values = await txn.get<RunTombstone | CleanupRecord | T>([
        TOMBSTONE_KEY,
        CLEANUP_RECORD_KEY,
        key,
      ]);
      const tombstone = this.tombstoneFrom(values as Map<string, RunTombstone | CleanupRecord>);
      if (tombstone) return expiredRead<T | null>(tombstone);
      return { ok: true, value: (values.get(key) as T | undefined) ?? null };
    });
  }

  /** Apply an event and capture all retention metadata in the same transaction. */
  async applyEvent(request: ApplyEventRequest): Promise<ApplyEventOutcome> {
    return await this.ctx.storage.transaction(async (txn) => {
      const now = new Date(this.now());
      const retention = await this.retentionState(txn, now.getTime());
      const { tombstone } = retention;
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
      const hookReferences: HookIndexReference[] = outcome.hookToIndex
        ? [
            {
              hookId: outcome.hookToIndex.hookId,
              token: outcome.hookToIndex.token,
            },
            ...outcome.releasedHooks,
          ]
        : outcome.releasedHooks;
      await putStorageEntries(
        txn,
        hookReferences.map((reference) => [hookMarkerKey(reference), reference] as const),
      );

      const retentionMs = request.cleanup?.retentionMs ?? 0;
      if (
        retentionMs > 0 &&
        outcome.run &&
        isTerminalWorkflowRunStatus(outcome.run.status) &&
        !retention.cleanup
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
          generation: 0,
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
      if (outcome.run && isTerminalWorkflowRunStatus(outcome.run.status)) {
        const terminalCleanup = await txn.get<TerminalCleanupRecord>(TERMINAL_CLEANUP_KEY);
        if (!terminalCleanup) {
          await txn.put<TerminalCleanupRecord>(TERMINAL_CLEANUP_KEY, {
            version: 1,
            runId: outcome.run.runId,
            phase: 'hooks',
            generation: 0,
            attempts: 0,
          });
        }
        await txn.setAlarm(this.now() + 1);
      }
      return finalizeEventPage(storeFrom(txn), outcome, request.params);
    });
  }

  async getRun(): Promise<RunReadOutcome<WorkflowRun | null>> {
    return this.readKey<WorkflowRun>('run');
  }

  async getStep(stepId: string): Promise<RunReadOutcome<Step | null>> {
    return this.readKey<Step>(`${STEP_KEY_PREFIX}${stepId}`);
  }

  async listSteps(params?: {
    limit?: number;
    cursor?: string;
    sortOrder?: 'asc' | 'desc';
  }): Promise<RunReadOutcome<{ data: Step[]; cursor: string | null; hasMore: boolean }>> {
    return this.read((txn) =>
      listByCreationTime<Step>(storeFrom(txn), STEP_CREATED_KEY_PREFIX, STEP_KEY_PREFIX, {
        limit: params?.limit ?? 20,
        cursor: params?.cursor,
        sortOrder: params?.sortOrder ?? 'asc',
      }),
    );
  }

  async getEvent(eventId: string): Promise<RunReadOutcome<Event | null>> {
    return this.readKey<Event>(`${EVENT_KEY_PREFIX}${eventId}`);
  }

  async listEvents(params?: {
    limit?: number;
    cursor?: string;
    sortOrder?: 'asc' | 'desc';
  }): Promise<RunReadOutcome<{ data: Event[]; cursor: string | null; hasMore: boolean }>> {
    return this.read((txn) =>
      listByPrefix<Event>(
        storeFrom(txn),
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
    return this.read((txn) =>
      listByCreationTime<Hook>(storeFrom(txn), HOOK_CREATED_KEY_PREFIX, HOOK_KEY_PREFIX, {
        limit: params?.limit ?? 100,
        cursor: params?.cursor,
        sortOrder: params?.sortOrder ?? 'asc',
      }),
    );
  }

  async getCleanupStatus(): Promise<CleanupRecord | null> {
    return (await this.ctx.storage.get<CleanupRecord>(CLEANUP_RECORD_KEY)) ?? null;
  }

  /** Explicitly schedule an existing terminal run, used for opt-in backfills. */
  async scheduleCleanup(request: ScheduleCleanupRequest): Promise<CleanupRecord | null> {
    if (request.retentionMs <= 0) return this.getCleanupStatus();
    return await this.ctx.storage.transaction(async (txn) => {
      const values = await txn.get<
        CleanupRecord | RunTombstone | TerminalCleanupRecord | WorkflowRun
      >([CLEANUP_RECORD_KEY, TOMBSTONE_KEY, TERMINAL_CLEANUP_KEY, 'run']);
      const existing = values.get(CLEANUP_RECORD_KEY) as CleanupRecord | undefined;
      if (existing) return existing;
      if (values.get(TOMBSTONE_KEY)) return null;
      const current = values.get('run') as WorkflowRun | undefined;
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
        generation: 0,
        attempts: 0,
        deletedPayloadKeys: 0,
        deletedStreams: 0,
        deletedQueueMessages: 0,
      };
      await txn.put('run', run);
      await txn.put(CLEANUP_RECORD_KEY, cleanup);
      await txn.setAlarm(
        values.get(TERMINAL_CLEANUP_KEY) || dueAt.getTime() <= this.now() ? this.now() + 1 : dueAt,
      );
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
      await txn.put(CLEANUP_RECORD_KEY, {
        ...cleanup,
        dueAt,
        generation: cleanup.generation + 1,
      });
      await txn.setAlarm(this.now() + 1);
    });
    await this.executeScheduledCleanupPage();
    return this.getCleanupStatus();
  }

  async rearmCleanup(): Promise<CleanupRecord | null> {
    const values = await this.ctx.storage.get<CleanupRecord | TerminalCleanupRecord>([
      CLEANUP_RECORD_KEY,
      TERMINAL_CLEANUP_KEY,
    ]);
    const cleanup = values.get(CLEANUP_RECORD_KEY) as CleanupRecord | undefined;
    const terminalCleanup = values.get(TERMINAL_CLEANUP_KEY) as TerminalCleanupRecord | undefined;
    if (terminalCleanup) {
      await this.ctx.storage.setAlarm(this.now() + 1);
    } else if (!cleanup || cleanup.phase === 'tombstoned') {
      await this.ctx.storage.deleteAlarm();
    } else {
      await this.ctx.storage.setAlarm(Math.max(this.now() + 1, cleanup.dueAt.getTime()));
    }
    return cleanup ?? null;
  }

  async alarm(): Promise<void> {
    await this.executeScheduledCleanupPage();
  }

  private namespace<T>(name: keyof WorkflowRunDOEnv): CellNamespace<T> {
    const namespace = (this.env as WorkflowRunDOEnv)?.[name];
    if (!namespace || typeof namespace === 'function') {
      throw new Error(`world-celld retention missing binding ${name}`);
    }
    return namespace as CellNamespace<T>;
  }

  private async setCleanupPhase(
    expected: CleanupRecord,
    next: CleanupRecord['phase'],
    updates: Partial<CleanupRecord> = {},
    continueAt?: number,
  ): Promise<void> {
    await this.ctx.storage.transaction(async (txn) => {
      const cleanup = await txn.get<CleanupRecord>(CLEANUP_RECORD_KEY);
      if (
        !cleanup ||
        cleanup.phase !== expected.phase ||
        cleanup.generation !== expected.generation
      ) {
        return;
      }
      await txn.put(CLEANUP_RECORD_KEY, {
        ...cleanup,
        ...updates,
        phase: next,
        generation: cleanup.generation + 1,
        attempts: 0,
        lastError: undefined,
      });
      if (continueAt !== undefined) await txn.setAlarm(continueAt);
    });
  }

  private async executeScheduledCleanupPage(): Promise<void> {
    const terminalCleanup = await this.ctx.storage.get<TerminalCleanupRecord>(TERMINAL_CLEANUP_KEY);
    if (terminalCleanup) {
      try {
        await this.executeTerminalCleanup(terminalCleanup);
      } catch (error) {
        await this.recordTerminalCleanupFailure(error, terminalCleanup);
      }
      return;
    }

    const cleanup = await this.ctx.storage.get<CleanupRecord>(CLEANUP_RECORD_KEY);
    if (!cleanup || cleanup.phase === 'tombstoned') {
      await this.ctx.storage.deleteAlarm();
      return;
    }
    try {
      await this.executeCleanup(cleanup);
    } catch (error) {
      await this.recordCleanupFailure(error, cleanup);
    }
  }

  private async executeCleanup(cleanup: CleanupRecord): Promise<void> {
    if (cleanup.phase === 'retained') {
      if (cleanup.dueAt.getTime() > this.now()) {
        await this.ctx.storage.setAlarm(cleanup.dueAt);
        return;
      }
      await this.setCleanupPhase(cleanup, 'index', {}, this.now() + 1);
      return;
    }

    if (cleanup.phase === 'index') {
      await this.deleteIndexPage(cleanup);
      return;
    }

    if (cleanup.phase === 'streams') {
      await this.deleteStreamPage(cleanup);
      return;
    }

    if (cleanup.phase === 'queues') {
      await this.deleteQueuePage(cleanup);
      return;
    }

    if (cleanup.phase === 'payload') {
      await this.deletePayloadBatch();
    }
  }

  private async executeTerminalCleanup(cleanup: TerminalCleanupRecord): Promise<void> {
    if (cleanup.phase === 'hooks') {
      await this.deleteTerminalHookPage(cleanup);
    } else {
      await this.deleteTerminalWaitPage(cleanup);
    }
  }

  private async deleteTerminalHookPage(cleanup: TerminalCleanupRecord): Promise<void> {
    const entries = await this.ctx.storage.list<Hook>({
      prefix: HOOK_KEY_PREFIX,
      limit: TERMINAL_HOOK_PAGE_SIZE + 1,
    });
    const page = Array.from(entries.values()).slice(0, TERMINAL_HOOK_PAGE_SIZE);
    const indexNamespace = this.namespace<IndexCleanupStub>('WORKFLOW_INDEX');
    const index = indexNamespace.get(indexNamespace.idFromName('index'));
    await index.releaseHookIndexes({
      runId: cleanup.runId,
      hooks: page.map((hook) => ({ hookId: hook.hookId, token: hook.token })),
      terminal: true,
    });

    await this.ctx.storage.transaction(async (txn) => {
      const current = await txn.get<TerminalCleanupRecord>(TERMINAL_CLEANUP_KEY);
      if (
        !current ||
        current.phase !== cleanup.phase ||
        current.generation !== cleanup.generation
      ) {
        return;
      }
      const keys = page.flatMap((hook) => [
        `${HOOK_KEY_PREFIX}${hook.hookId}`,
        hookCreationIndexKey(hook),
      ]);
      if (keys.length > 0) await txn.delete(keys);
      await txn.put<TerminalCleanupRecord>(TERMINAL_CLEANUP_KEY, {
        ...current,
        phase: entries.size <= TERMINAL_HOOK_PAGE_SIZE ? 'waits' : 'hooks',
        generation: current.generation + 1,
        attempts: 0,
        lastError: undefined,
      });
      await txn.setAlarm(this.now() + 1);
    });
  }

  private async deleteTerminalWaitPage(cleanup: TerminalCleanupRecord): Promise<void> {
    await this.ctx.storage.transaction(async (txn) => {
      const current = await txn.get<TerminalCleanupRecord>(TERMINAL_CLEANUP_KEY);
      if (
        !current ||
        current.phase !== cleanup.phase ||
        current.generation !== cleanup.generation
      ) {
        return;
      }
      const entries = await txn.list({
        prefix: WAIT_KEY_PREFIX,
        limit: TERMINAL_WAIT_PAGE_SIZE + 1,
      });
      const keys = Array.from(entries.keys()).slice(0, TERMINAL_WAIT_PAGE_SIZE);
      if (keys.length > 0) await txn.delete(keys);

      if (entries.size > TERMINAL_WAIT_PAGE_SIZE) {
        await txn.put<TerminalCleanupRecord>(TERMINAL_CLEANUP_KEY, {
          ...current,
          generation: current.generation + 1,
          attempts: 0,
          lastError: undefined,
        });
        await txn.setAlarm(this.now() + 1);
        return;
      }

      await txn.delete(TERMINAL_CLEANUP_KEY);
      const retention = await txn.get<CleanupRecord>(CLEANUP_RECORD_KEY);
      if (retention && retention.phase !== 'tombstoned') {
        await txn.setAlarm(Math.max(this.now() + 1, retention.dueAt.getTime()));
      } else {
        await txn.deleteAlarm();
      }
    });
  }

  private async deleteIndexPage(cleanup: CleanupRecord): Promise<void> {
    const hookEntries = await this.ctx.storage.list<HookIndexReference>({
      prefix: HOOK_MARKER_PREFIX,
      limit: HOOK_MARKER_PAGE_SIZE + 1,
    });
    const hookMarkers = Array.from(hookEntries).slice(0, HOOK_MARKER_PAGE_SIZE);
    const indexNamespace = this.namespace<IndexCleanupStub>('WORKFLOW_INDEX');
    const index = indexNamespace.get(indexNamespace.idFromName('index'));
    await index.expireRun({
      runId: cleanup.runId,
      keys: [workflowRunIndexKey(cleanup), globalRunIndexKey(cleanup)],
      hooks: hookMarkers.map(([, value]) => value),
      expiredAt: cleanup.dueAt.getTime(),
    });
    const markerKeys = hookMarkers.map(([key]) => key);
    await this.ctx.storage.transaction(async (txn) => {
      const current = await txn.get<CleanupRecord>(CLEANUP_RECORD_KEY);
      if (current?.phase !== 'index' || current.generation !== cleanup.generation) return;
      if (markerKeys.length > 0) await txn.delete(markerKeys);
      if (hookEntries.size <= HOOK_MARKER_PAGE_SIZE) {
        await txn.put(CLEANUP_RECORD_KEY, {
          ...current,
          phase: 'streams',
          generation: current.generation + 1,
          attempts: 0,
          lastError: undefined,
        });
      } else {
        await txn.put(CLEANUP_RECORD_KEY, {
          ...current,
          generation: current.generation + 1,
          attempts: 0,
          lastError: undefined,
        });
      }
      await txn.setAlarm(this.now() + 1);
    });
  }

  private async deleteStreamPage(cleanup: CleanupRecord): Promise<void> {
    const streamsNamespace = this.namespace<StreamCleanupStub>('WORKFLOW_STREAMS');
    const registry = streamsNamespace.get(
      streamsNamespace.idFromName(`run-streams:${cleanup.runId}`),
    );
    const { streams } = await registry.expireRegistry(cleanup.runId, cleanup.dueAt.getTime(), {
      limit: STREAMS_PER_CLEANUP_PAGE,
    });
    const completed: string[] = [];
    let remainingChunks = STREAM_CHUNKS_PER_CLEANUP_PAGE;
    let remainingBytes = STREAM_BYTES_PER_CLEANUP_PAGE;
    for (const name of streams) {
      if (remainingChunks <= 0 || remainingBytes <= 0) break;
      const stream = streamsNamespace.get(streamsNamespace.idFromName(`stream:${name}`));
      const result = await stream.expireStream(cleanup.runId, cleanup.dueAt.getTime(), {
        limit: remainingChunks,
        byteLimit: remainingBytes,
      });
      remainingChunks -= result.chunks;
      remainingBytes -= result.bytes;
      if (result.done) completed.push(name);
    }
    const registryResult = await registry.finalizeRegistry(cleanup.runId, completed);
    await this.ctx.storage.transaction(async (txn) => {
      const current = await txn.get<CleanupRecord>(CLEANUP_RECORD_KEY);
      if (current?.phase !== 'streams' || current.generation !== cleanup.generation) return;
      await txn.put(CLEANUP_RECORD_KEY, {
        ...current,
        deletedStreams: registryResult.deleted,
        phase: registryResult.done ? 'queues' : 'streams',
        generation: current.generation + 1,
        attempts: 0,
        lastError: undefined,
      });
      await txn.setAlarm(this.now() + 1);
    });
  }

  private async deleteQueuePage(cleanup: CleanupRecord): Promise<void> {
    const queues = this.namespace<QueueCleanupStub>('WORKFLOW_QUEUE');
    let progress =
      (await this.ctx.storage.get<CleanupProgress>(CLEANUP_PROGRESS_KEY)) ??
      ({ queueShard: 0, queueShardDeleted: 0 } satisfies CleanupProgress);
    let remainingShards = QUEUE_SHARDS_PER_CLEANUP_PAGE;
    let expectedGeneration = cleanup.generation;

    while (progress.queueShard < cleanup.queueShards && remainingShards-- > 0) {
      const requestedShard = progress.queueShard;
      const queue = queues.get(queues.idFromName(`q:${requestedShard}`));
      const result = await queue.expireRun(cleanup.runId, cleanup.dueAt.getTime(), {
        limit: QUEUE_REFERENCES_PER_CLEANUP_PAGE,
      });
      const reconciled = await this.ctx.storage.transaction(async (txn) => {
        const current = await txn.get<CleanupRecord>(CLEANUP_RECORD_KEY);
        if (current?.phase !== 'queues') return null;
        if (current.generation !== expectedGeneration) {
          return { stale: true as const };
        }
        const persisted =
          (await txn.get<CleanupProgress>(CLEANUP_PROGRESS_KEY)) ??
          ({ queueShard: 0, queueShardDeleted: 0 } satisfies CleanupProgress);
        if (persisted.queueShard !== requestedShard) {
          return { stale: true as const };
        }
        if (result.deleted < persisted.queueShardDeleted) {
          return { stale: true as const };
        }
        const next: CleanupProgress = result.done
          ? { queueShard: persisted.queueShard + 1, queueShardDeleted: 0 }
          : { ...persisted, queueShardDeleted: result.deleted };
        const delta = result.deleted - persisted.queueShardDeleted;
        await txn.put(CLEANUP_RECORD_KEY, {
          ...current,
          deletedQueueMessages: current.deletedQueueMessages + delta,
          generation: current.generation + 1,
          attempts: 0,
          lastError: undefined,
        });
        await txn.put(CLEANUP_PROGRESS_KEY, next);
        await txn.setAlarm(this.now() + 1);
        return {
          stale: false as const,
          progress: next,
          generation: current.generation + 1,
          applied:
            result.done ||
            next.queueShard !== persisted.queueShard ||
            next.queueShardDeleted !== persisted.queueShardDeleted,
        };
      });
      if (!reconciled) return;
      if (reconciled.stale) return;
      progress = reconciled.progress;
      expectedGeneration = reconciled.generation;
      if (!reconciled.applied && progress.queueShard === requestedShard) break;
      if (!result.done && progress.queueShard === requestedShard) break;
    }

    await this.ctx.storage.transaction(async (txn) => {
      const current = await txn.get<CleanupRecord>(CLEANUP_RECORD_KEY);
      if (current?.phase !== 'queues') return;
      const persisted =
        (await txn.get<CleanupProgress>(CLEANUP_PROGRESS_KEY)) ??
        ({ queueShard: 0, queueShardDeleted: 0 } satisfies CleanupProgress);
      if (persisted.queueShard >= current.queueShards) {
        await txn.put(CLEANUP_RECORD_KEY, {
          ...current,
          phase: 'payload',
          generation: current.generation + 1,
        });
        await txn.delete(CLEANUP_PROGRESS_KEY);
        await txn.setAlarm(this.now() + 1);
      }
    });
  }

  private async deletePayloadBatch(): Promise<boolean> {
    return await this.ctx.storage.transaction(async (txn) => {
      const cleanup = await txn.get<CleanupRecord>(CLEANUP_RECORD_KEY);
      if (!cleanup || cleanup.phase !== 'payload') return true;
      const entries = await txn.list({ limit: PAYLOAD_DELETE_BATCH + 3, noCache: true });
      const keys = Array.from(entries.keys())
        .filter((key) => key !== CLEANUP_RECORD_KEY && key !== TOMBSTONE_KEY)
        .slice(0, PAYLOAD_DELETE_BATCH);
      const now = new Date(this.now());
      await txn.put(TOMBSTONE_KEY, cleanupTombstone(cleanup, now));
      if (keys.length > 0) await txn.delete(keys);

      if (keys.length > 0) {
        await txn.put(CLEANUP_RECORD_KEY, {
          ...cleanup,
          deletedPayloadKeys: cleanup.deletedPayloadKeys + keys.length,
          generation: cleanup.generation + 1,
          lastError: undefined,
        });
        await txn.setAlarm(this.now() + 1);
        return false;
      }

      await txn.put(CLEANUP_RECORD_KEY, {
        ...cleanup,
        phase: 'tombstoned',
        generation: cleanup.generation + 1,
        attempts: 0,
        lastError: undefined,
        tombstonedAt: now,
      });
      await txn.deleteAlarm();
      return true;
    });
  }

  private async recordCleanupFailure(error: unknown, expected: CleanupRecord): Promise<void> {
    await this.ctx.storage.transaction(async (txn) => {
      const cleanup = await txn.get<CleanupRecord>(CLEANUP_RECORD_KEY);
      if (
        !cleanup ||
        cleanup.phase !== expected.phase ||
        cleanup.generation !== expected.generation ||
        cleanup.phase === 'tombstoned'
      ) {
        return;
      }
      const attempts = cleanup.attempts + 1;
      const delay = Math.min(CLEANUP_RETRY_MAX_MS, 1000 * 2 ** Math.min(attempts - 1, 12));
      await txn.put(CLEANUP_RECORD_KEY, {
        ...cleanup,
        generation: cleanup.generation + 1,
        attempts,
        lastError: error instanceof Error ? error.message : String(error),
        lastAttemptAt: new Date(this.now()),
      });
      await txn.setAlarm(this.now() + delay);
    });
  }

  private async recordTerminalCleanupFailure(
    error: unknown,
    expected: TerminalCleanupRecord,
  ): Promise<void> {
    await this.ctx.storage.transaction(async (txn) => {
      const cleanup = await txn.get<TerminalCleanupRecord>(TERMINAL_CLEANUP_KEY);
      if (
        !cleanup ||
        cleanup.phase !== expected.phase ||
        cleanup.generation !== expected.generation
      ) {
        return;
      }
      const attempts = cleanup.attempts + 1;
      const delay = Math.min(CLEANUP_RETRY_MAX_MS, 1000 * 2 ** Math.min(attempts - 1, 12));
      await txn.put<TerminalCleanupRecord>(TERMINAL_CLEANUP_KEY, {
        ...cleanup,
        generation: cleanup.generation + 1,
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
