/**
 * Test mocks for Node.js environment.
 *
 * The mock Durable Object stubs delegate to the SAME transactional
 * event-application core (`apply-event.ts`) as the real WorkflowRunDO, so
 * unit tests exercise the real guard/idempotency/pagination logic — only the
 * storage medium (an in-memory sorted map) is mocked.
 */

import {
  isTerminalWorkflowRunStatus,
  slotToEventId,
  type Event,
  type Hook,
  type Step,
  type WorkflowRun,
} from '@workflow/world';
import type { HookTokenOwner } from './config.js';
import type { HookReservation, HookReservationResult } from './indexes.js';
import type { EnqueueOutcome, EnqueueRequest, QueueCellStub } from './queue.js';
import {
  normalizeStreamError,
  validateStreamReadRequest,
  validateStreamWriteChunks,
  type StreamErrorData,
  type StreamReadRequest,
  type StreamReadResult,
  type StreamTerminalState,
  type StreamWriteResult,
} from './stream-protocol.js';
import {
  applyEvent,
  finalizeEventPage,
  hookClaimCancellationKey,
  type ApplyEventOutcome,
  type ApplyEventRequest,
  EVENT_KEY_PREFIX,
  type EventStore,
  type EventStoreListOptions,
  HOOK_CREATED_KEY_PREFIX,
  HOOK_KEY_PREFIX,
  listByCreationTime,
  listByPrefix,
  STEP_CREATED_KEY_PREFIX,
  STEP_KEY_PREFIX,
} from './apply-event.js';
import { parse } from './vendor/shared/index.js';
import { globalRunIndexKey, workflowRunIndexKey } from './retention.js';
import type {
  CleanupRecord,
  ExpireRunIndexesRequest,
  ReleaseHookIndexesRequest,
  ReleaseHookIndexesResult,
  RunReadOutcome,
  ScheduleCleanupRequest,
} from './retention.js';

// In-memory storage for mock Durable Objects
const durableObjectData = new Map<string, Map<string, unknown>>();
const kvData = new Map<string, string>();

// Get storage for a specific DO instance
const getDOStorage = (doId: string): Map<string, unknown> => {
  let storage = durableObjectData.get(doId);
  if (!storage) {
    storage = new Map();
    durableObjectData.set(doId, storage);
  }
  return storage;
};

/**
 * In-memory {@link EventStore} over a Map, with lexicographically ordered
 * listing to match Durable Object storage semantics. The backing map is
 * resolved lazily on every operation so stubs survive clearMockData().
 */
function createMemoryStore(getData: () => Map<string, unknown>): EventStore {
  return {
    async get<T>(key: string): Promise<T | undefined> {
      return getData().get(key) as T | undefined;
    },
    async put<T>(key: string, value: T): Promise<void> {
      getData().set(key, value);
    },
    async delete(key: string): Promise<boolean> {
      return getData().delete(key);
    },
    async list<T>(options: EventStoreListOptions): Promise<Map<string, T>> {
      const data = getData();
      let keys = Array.from(data.keys())
        .filter((key) => key.startsWith(options.prefix))
        .toSorted();
      if (options.startAfter !== undefined) {
        const bound = options.startAfter;
        keys = keys.filter((key) => key > bound);
      }
      if (options.end !== undefined) {
        const bound = options.end;
        keys = keys.filter((key) => key < bound);
      }
      if (options.reverse) {
        keys = keys.toReversed();
      }
      if (options.limit !== undefined) {
        keys = keys.slice(0, options.limit);
      }
      return new Map(keys.map((key) => [key, data.get(key) as T]));
    },
  };
}

interface InflightClaim {
  messageId: string;
  claimedAt: number;
}

interface MockHookClaim {
  owner: HookTokenOwner;
  claimId: string;
}

/**
 * Mock Durable Object stub with the same RPC surface as WorkflowRunDO.
 */
class MockWorkflowRunDOStub {
  private store: EventStore;
  private applyChain: Promise<void> = Promise.resolve();

  constructor(runId: string) {
    this.store = createMemoryStore(() => getDOStorage(runId));
  }

  async applyEvent(request: ApplyEventRequest): Promise<ApplyEventOutcome> {
    const result = this.applyChain.then(() => this.applyEventSerial(request));
    this.applyChain = result.then(
      () => undefined,
      () => undefined,
    );
    return result;
  }

  private async applyEventSerial(request: ApplyEventRequest): Promise<ApplyEventOutcome> {
    let eventSequence = (await this.store.get<number>('event_sequence')) ?? 0;
    const outcome = await applyEvent(this.store, {
      ...request,
      nextEventId: () => slotToEventId(++eventSequence),
      now: new Date(),
    });
    if (outcome.ok) {
      await this.store.put('event_sequence', eventSequence);
    }
    return finalizeEventPage(this.store, outcome, request.params);
  }

  async resolveHookTokenClaim(request: {
    hookId: string;
    token: string;
    claimId: string;
  }): Promise<{ committed: boolean }> {
    const result = this.applyChain.then(async () => {
      const hook = await this.store.get<Hook>(`${HOOK_KEY_PREFIX}${request.hookId}`);
      if (hook?.token === request.token) return { committed: true };
      await this.store.put(hookClaimCancellationKey(request.claimId), {
        hookId: request.hookId,
        token: request.token,
        canceledAt: Date.now(),
      });
      return { committed: false };
    });
    this.applyChain = result.then(
      () => undefined,
      () => undefined,
    );
    return result;
  }

  async getRun(): Promise<RunReadOutcome<WorkflowRun | null>> {
    const run = await this.store.get<WorkflowRun>('run');
    return { ok: true, value: run ?? null };
  }

  async getStep(stepId: string): Promise<RunReadOutcome<Step | null>> {
    const step = await this.store.get<Step>(`${STEP_KEY_PREFIX}${stepId}`);
    return { ok: true, value: step ?? null };
  }

  async getEvent(eventId: string): Promise<RunReadOutcome<Event | null>> {
    const event = await this.store.get<Event>(`${EVENT_KEY_PREFIX}${eventId}`);
    return { ok: true, value: event ?? null };
  }

  async listEvents(params?: {
    limit?: number;
    cursor?: string;
    sortOrder?: 'asc' | 'desc';
  }): Promise<RunReadOutcome<{ data: Event[]; cursor: string | null; hasMore: boolean }>> {
    return {
      ok: true,
      value: await listByPrefix<Event>(
        this.store,
        EVENT_KEY_PREFIX,
        {
          limit: params?.limit ?? 100,
          cursor: params?.cursor,
          sortOrder: params?.sortOrder ?? 'asc',
        },
        (event) => event.eventId,
      ),
    };
  }

  async listSteps(params?: {
    limit?: number;
    cursor?: string;
    sortOrder?: 'asc' | 'desc';
  }): Promise<RunReadOutcome<{ data: Step[]; cursor: string | null; hasMore: boolean }>> {
    return {
      ok: true,
      value: await listByCreationTime<Step>(this.store, STEP_CREATED_KEY_PREFIX, STEP_KEY_PREFIX, {
        limit: params?.limit ?? 20,
        cursor: params?.cursor,
        sortOrder: params?.sortOrder ?? 'asc',
      }),
    };
  }

  async listHooks(params?: {
    limit?: number;
    cursor?: string;
    sortOrder?: 'asc' | 'desc';
  }): Promise<RunReadOutcome<{ data: Hook[]; cursor: string | null; hasMore: boolean }>> {
    return {
      ok: true,
      value: await listByCreationTime<Hook>(this.store, HOOK_CREATED_KEY_PREFIX, HOOK_KEY_PREFIX, {
        limit: params?.limit ?? 100,
        cursor: params?.cursor,
        sortOrder: params?.sortOrder ?? 'asc',
      }),
    };
  }

  async getCleanupStatus(): Promise<CleanupRecord | null> {
    return null;
  }

  async scheduleCleanup(_request: ScheduleCleanupRequest): Promise<CleanupRecord | null> {
    return null;
  }

  async cleanupNow(_request: ScheduleCleanupRequest): Promise<CleanupRecord | null> {
    return null;
  }

  async rearmCleanup(): Promise<CleanupRecord | null> {
    return null;
  }

  async claimInflight(params: { messageId: string; staleMs: number }): Promise<{
    claimed: boolean;
  }> {
    const existing = await this.store.get<InflightClaim>('claim');
    const now = Date.now();
    if (
      existing &&
      existing.messageId !== params.messageId &&
      now - existing.claimedAt < params.staleMs
    ) {
      return { claimed: false };
    }
    await this.store.put<InflightClaim>('claim', { messageId: params.messageId, claimedAt: now });
    return { claimed: true };
  }

  async releaseInflight(): Promise<void> {
    await this.store.delete('claim');
  }
}

/**
 * In-memory composite workflow index with real cursor pagination semantics: the cursor
 * continues after the last key returned by the previous list call, and
 * `list_complete` reflects whether further keys exist.
 */
class MockWorkflowIndex {
  async commitRun(run: WorkflowRun, serializedMetadata: string): Promise<{ stored: boolean }> {
    if (kvData.has(`expired:${run.runId}`)) return { stored: false };
    if (isTerminalWorkflowRunStatus(run.status)) {
      kvData.set(`terminal:${run.runId}`, String(Date.now()));
    }
    kvData.set(workflowRunIndexKey(run), serializedMetadata);
    kvData.set(globalRunIndexKey(run), serializedMetadata);
    return { stored: true };
  }

  async listRuns(options?: {
    prefix?: string;
    cursor?: string;
    limit?: number;
    reverse?: boolean;
  }): Promise<{
    keys: Array<{ name: string; value: string }>;
    list_complete: boolean;
    cursor?: string;
  }> {
    const prefix = options?.prefix || '';
    const limit = options?.limit || 1000;

    let matchingKeys = Array.from(kvData.keys())
      .filter((key) => key.startsWith(prefix))
      .toSorted();

    if (options?.cursor) {
      const cursor = options.cursor;
      matchingKeys = matchingKeys.filter((key) => (options.reverse ? key < cursor : key > cursor));
    }

    if (options?.reverse) {
      matchingKeys = matchingKeys.toReversed();
    }

    const page = matchingKeys.slice(0, limit);
    const listComplete = matchingKeys.length <= limit;

    return {
      keys: page.map((name) => ({ name, value: kvData.get(name)! })),
      list_complete: listComplete,
      cursor: listComplete ? undefined : page.at(-1),
    };
  }

  private async getHook(key: string): Promise<string | null> {
    const value = kvData.get(key);
    if (!value) return null;
    const hook = parse<Hook>(value);
    if (kvData.has(`terminal:${hook.runId}`) || kvData.has(`expired:${hook.runId}`)) return null;
    return value;
  }

  getHookByToken(token: string): Promise<string | null> {
    return this.getHook(`hook:${token}`);
  }

  getHookById(hookId: string): Promise<string | null> {
    return this.getHook(`hookid:${hookId}`);
  }

  async expireRun(request: ExpireRunIndexesRequest): Promise<{ deleted: number }> {
    kvData.set(`expired:${request.runId}`, String(request.expiredAt));
    let deleted = 0;
    if (kvData.delete(`terminal:${request.runId}`)) deleted++;
    for (const key of request.keys) {
      if (kvData.delete(key)) deleted++;
    }
    for (const hook of request.hooks) {
      for (const key of [
        `hook:${hook.token}`,
        `hookid:${hook.hookId}`,
        `hookclaim:${hook.token}`,
        `hookidclaim:${hook.hookId}`,
      ]) {
        if (kvData.delete(key)) deleted++;
      }
    }
    return { deleted };
  }

  async reserveHook(token: string, owner: HookTokenOwner): Promise<HookReservationResult> {
    const raw = kvData.get(`hook:${token}`);
    let tokenClaimId: string | undefined;
    if (raw !== undefined) {
      const hook = parse<Hook>(raw);
      const holder = { runId: hook.runId, hookId: hook.hookId };
      if (holder.runId !== owner.runId || holder.hookId !== owner.hookId) {
        return { admitted: false, holder };
      }
    } else {
      const claimKey = `hookclaim:${token}`;
      const claim = kvData.get(claimKey);
      if (claim !== undefined) {
        const existing = JSON.parse(claim) as MockHookClaim;
        if (existing.owner.runId !== owner.runId || existing.owner.hookId !== owner.hookId) {
          return { admitted: false, holder: existing.owner };
        }
        tokenClaimId = existing.claimId;
      } else {
        tokenClaimId = crypto.randomUUID();
        kvData.set(claimKey, JSON.stringify({ owner, claimId: tokenClaimId }));
      }
    }

    const idRaw = kvData.get(`hookid:${owner.hookId}`);
    if (idRaw !== undefined) {
      const hook = parse<Hook>(idRaw);
      const holder = { runId: hook.runId, hookId: hook.hookId };
      if (tokenClaimId) kvData.delete(`hookclaim:${token}`);
      return { admitted: false, holder };
    }
    const idClaimKey = `hookidclaim:${owner.hookId}`;
    const idClaimRaw = kvData.get(idClaimKey);
    let hookIdClaimId: string;
    if (idClaimRaw !== undefined) {
      const existing = JSON.parse(idClaimRaw) as MockHookClaim;
      if (existing.owner.runId !== owner.runId || existing.owner.hookId !== owner.hookId) {
        if (tokenClaimId) kvData.delete(`hookclaim:${token}`);
        return { admitted: false, holder: existing.owner };
      }
      hookIdClaimId = tokenClaimId ?? existing.claimId;
      if (hookIdClaimId !== existing.claimId) {
        kvData.set(idClaimKey, JSON.stringify({ owner, claimId: hookIdClaimId }));
      }
    } else {
      hookIdClaimId = tokenClaimId ?? crypto.randomUUID();
      kvData.set(idClaimKey, JSON.stringify({ owner, claimId: hookIdClaimId }));
    }
    return {
      admitted: true,
      reservation: {
        claimId: `${tokenClaimId ?? '-'}:${hookIdClaimId}`,
        tokenClaimId,
        hookIdClaimId,
      },
    };
  }

  async finalizeHookIndexes(
    token: string,
    hookId: string,
    serializedHook: string,
    owner: HookTokenOwner,
    reservation?: HookReservation,
  ): Promise<void> {
    if (kvData.has(`terminal:${owner.runId}`) || kvData.has(`expired:${owner.runId}`)) {
      if (reservation) await this.releaseHookReservation(token, owner, reservation);
      return;
    }
    const raw = kvData.get(`hook:${token}`);
    if (raw !== undefined) {
      const hook = parse<Hook>(raw);
      if (hook.runId !== owner.runId || hook.hookId !== owner.hookId) {
        throw new Error(`Hook token ${token} is owned by another hook`);
      }
    }
    const claim = kvData.get(`hookclaim:${token}`);
    if (claim !== undefined) {
      const existing = JSON.parse(claim) as MockHookClaim;
      if (
        existing.owner.runId !== owner.runId ||
        existing.owner.hookId !== owner.hookId ||
        existing.claimId !== reservation?.tokenClaimId
      ) {
        throw new Error(`Hook token ${token} is reserved by another hook`);
      }
    } else if (raw === undefined) {
      throw new Error(`Hook token ${token} has no active reservation`);
    }
    const idClaim = kvData.get(`hookidclaim:${hookId}`);
    const idRaw = kvData.get(`hookid:${hookId}`);
    if (idRaw !== undefined) {
      const holder = parse<Hook>(idRaw);
      if (holder.runId !== owner.runId || holder.hookId !== owner.hookId) {
        throw new Error(`Hook id ${hookId} is owned by another hook`);
      }
    }
    if (idClaim !== undefined) {
      const existing = JSON.parse(idClaim) as MockHookClaim;
      if (
        existing.owner.runId !== owner.runId ||
        existing.owner.hookId !== owner.hookId ||
        existing.claimId !== reservation?.hookIdClaimId
      ) {
        throw new Error(`Hook id ${hookId} is reserved by another hook`);
      }
    } else if (idRaw === undefined) {
      throw new Error(`Hook id ${hookId} has no active reservation`);
    }
    kvData.set(`hook:${token}`, serializedHook);
    kvData.set(`hookid:${hookId}`, serializedHook);
    kvData.delete(`hookclaim:${token}`);
    kvData.delete(`hookidclaim:${hookId}`);
  }

  async releaseHookReservation(
    token: string,
    owner: HookTokenOwner,
    reservation: HookReservation,
  ): Promise<void> {
    for (const [key, claimId] of [
      [`hookclaim:${token}`, reservation.tokenClaimId],
      [`hookidclaim:${owner.hookId}`, reservation.hookIdClaimId],
    ] as const) {
      const claim = kvData.get(key);
      if (claim !== undefined && claimId !== undefined) {
        const existing = JSON.parse(claim) as MockHookClaim;
        if (
          existing.owner.runId === owner.runId &&
          existing.owner.hookId === owner.hookId &&
          existing.claimId === claimId
        ) {
          kvData.delete(key);
        }
      }
    }
  }

  async releaseHookIndexes(request: ReleaseHookIndexesRequest): Promise<ReleaseHookIndexesResult> {
    if (request.terminal) kvData.set(`terminal:${request.runId}`, String(Date.now()));
    let deleted = 0;
    for (const { token, hookId } of request.hooks) {
      const owner = { runId: request.runId, hookId };
      for (const key of [`hook:${token}`, `hookid:${hookId}`]) {
        const raw = kvData.get(key);
        if (raw !== undefined) {
          const hook = parse<Hook>(raw);
          if (hook.runId === owner.runId && hook.hookId === owner.hookId) {
            deleted += Number(kvData.delete(key));
          }
        }
      }
      const claimKey = `hookclaim:${token}`;
      const claim = kvData.get(claimKey);
      if (claim !== undefined) {
        const existing = JSON.parse(claim) as MockHookClaim;
        if (existing.owner.runId === owner.runId && existing.owner.hookId === owner.hookId) {
          deleted += Number(kvData.delete(claimKey));
        }
      }
      const idClaimKey = `hookidclaim:${hookId}`;
      const idClaim = kvData.get(idClaimKey);
      if (idClaim !== undefined) {
        const existing = JSON.parse(idClaim) as MockHookClaim;
        if (existing.owner.runId === owner.runId && existing.owner.hookId === owner.hookId) {
          deleted += Number(kvData.delete(idClaimKey));
        }
      }
    }
    return { deleted };
  }
}

/**
 * Mock StreamDO stub with the same RPC surface as the real StreamDO
 * (chunk storage + per-run stream registry roles).
 */
class MockStreamDOStub {
  private chunks: Uint8Array[] = [];
  private state: StreamTerminalState = 'open';
  private error: StreamErrorData | undefined;
  private registry = new Set<string>();
  private ownerRunId: string | undefined;
  private expired = false;
  private registryDeleted = 0;
  private waiters = new Set<() => void>();

  private wake(): void {
    for (const resolve of Array.from(this.waiters)) resolve();
  }

  async writeChunks(runId: string, data: Uint8Array[]): Promise<StreamWriteResult> {
    validateStreamWriteChunks(data);
    if (this.expired) throw new Error('Stream has expired');
    if (this.ownerRunId && this.ownerRunId !== runId) throw new Error('Stream owner mismatch');
    this.ownerRunId = runId;
    if (this.state !== 'open') {
      throw new Error('Cannot write to a closed stream');
    }
    const startIndex = this.chunks.length;
    this.chunks.push(...data.map((chunk) => new Uint8Array(chunk)));
    this.wake();
    return { startIndex, count: data.length, tailIndex: this.chunks.length - 1 };
  }

  async closeStream(runId: string): Promise<void> {
    if (this.expired) throw new Error('Stream has expired');
    if (this.ownerRunId && this.ownerRunId !== runId) throw new Error('Stream owner mismatch');
    this.ownerRunId = runId;
    if (this.state === 'open') this.state = 'closed';
    this.wake();
  }

  async failStream(runId: string, error: StreamErrorData | string): Promise<void> {
    if (this.expired) throw new Error('Stream has expired');
    if (this.ownerRunId && this.ownerRunId !== runId) throw new Error('Stream owner mismatch');
    this.ownerRunId = runId;
    if (this.state === 'open') {
      this.state = 'errored';
      this.error = normalizeStreamError(error);
    }
    this.wake();
  }

  async readChunks(request: StreamReadRequest, signal?: AbortSignal): Promise<StreamReadResult> {
    validateStreamReadRequest(request);
    const snapshot = (timedOut: boolean): StreamReadResult => {
      let bytes = 0;
      const chunks: Uint8Array[] = [];
      if (this.state !== 'expired') {
        for (const chunk of this.chunks.slice(
          request.startIndex,
          request.startIndex + request.maxChunks,
        )) {
          if (bytes + chunk.byteLength > request.maxBytes) break;
          chunks.push(structuredClone(chunk));
          bytes += chunk.byteLength;
        }
      }
      return {
        startIndex: request.startIndex,
        tailIndex: this.chunks.length - 1,
        chunks,
        state: this.state,
        timedOut,
        error: this.error,
      };
    };
    const immediate = snapshot(false);
    if (
      immediate.chunks.length > 0 ||
      immediate.state !== 'open' ||
      request.waitMs === 0 ||
      request.maxChunks === 0
    ) {
      return immediate;
    }
    if (signal?.aborted) throw signal.reason;

    const timedOut = await new Promise<boolean>((resolve, reject) => {
      let settled = false;
      const finish = (timeout: boolean) => {
        if (settled) return;
        settled = true;
        clearTimeout(timer);
        this.waiters.delete(onChange);
        signal?.removeEventListener('abort', onAbort);
        resolve(timeout);
      };
      const onChange = () => finish(false);
      const onAbort = () => {
        if (settled) return;
        settled = true;
        clearTimeout(timer);
        this.waiters.delete(onChange);
        reject(signal?.reason);
      };
      const timer = setTimeout(() => finish(true), request.waitMs);
      this.waiters.add(onChange);
      signal?.addEventListener('abort', onAbort, { once: true });
    });
    return snapshot(timedOut);
  }

  async registerStream(runId: string, name: string): Promise<void> {
    if (this.expired) throw new Error('Stream registry has expired');
    if (this.ownerRunId && this.ownerRunId !== runId) throw new Error('Registry owner mismatch');
    this.ownerRunId = runId;
    this.registry.add(name);
  }

  async listStreams(): Promise<string[]> {
    if (this.expired) throw new Error('Stream registry has expired');
    return Array.from(this.registry).toSorted();
  }

  async expireRegistry(
    runId: string,
    _expiredAt: number,
    options?: { limit?: number },
  ): Promise<{ streams: string[] }> {
    if (this.ownerRunId && this.ownerRunId !== runId) throw new Error('Registry owner mismatch');
    this.ownerRunId = runId;
    this.expired = true;
    const streams = Array.from(this.registry);
    return { streams: streams.slice(0, options?.limit ?? streams.length) };
  }

  async finalizeRegistry(
    runId: string,
    streams: string[],
  ): Promise<{ deleted: number; done: boolean }> {
    if (this.ownerRunId !== runId || !this.expired) throw new Error('Registry is not expired');
    for (const stream of streams) this.registryDeleted += Number(this.registry.delete(stream));
    return { deleted: this.registryDeleted, done: this.registry.size === 0 };
  }

  async expireStream(
    runId: string,
    _expiredAt: number,
    options?: { limit?: number; byteLimit?: number },
  ): Promise<{ deleted: boolean; chunks: number; bytes: number; done: boolean }> {
    if (this.ownerRunId && this.ownerRunId !== runId) throw new Error('Stream owner mismatch');
    const firstFence = !this.expired;
    const limit = Math.min(this.chunks.length, options?.limit ?? this.chunks.length);
    const byteLimit = options?.byteLimit ?? Number.POSITIVE_INFINITY;
    let chunks = 0;
    let bytes = 0;
    for (const chunk of this.chunks.slice(0, limit)) {
      if (chunks > 0 && bytes + chunk.byteLength > byteLimit) break;
      chunks++;
      bytes += chunk.byteLength;
    }
    this.ownerRunId = runId;
    this.chunks.splice(0, chunks);
    this.state = 'expired';
    this.expired = true;
    this.wake();
    return { deleted: firstFence, chunks, bytes, done: this.chunks.length === 0 };
  }
}

/** Enqueues recorded by mock queue cells (for assertions in tests). */
export const recordedEnqueues: Array<EnqueueRequest & { cellName: string }> = [];

/**
 * Mock QueueDO stub: records enqueues and mirrors the cell's idempotencyKey
 * dedup semantics (same key while active -> original messageId, deduped).
 */
class MockQueueCellStub implements QueueCellStub {
  private activeKeys = new Map<string, string>();

  constructor(private cellName: string) {}

  async enqueue(request: EnqueueRequest): Promise<EnqueueOutcome> {
    if (request.idempotencyKey) {
      const existing = this.activeKeys.get(request.idempotencyKey);
      if (existing) {
        return { ok: true, messageId: existing, deduped: true };
      }
      this.activeKeys.set(request.idempotencyKey, request.messageId);
    }
    recordedEnqueues.push({ ...request, cellName: this.cellName });
    return { ok: true, messageId: request.messageId, deduped: false };
  }

  async expireRun(): Promise<{ deleted: number; done: boolean }> {
    return { deleted: 0, done: true };
  }
}

/**
 * Mock Durable Object Namespace
 */
class MockDurableObjectNamespace<T> {
  private stubs = new Map<string, T>();

  constructor(private createStub: (name: string) => T) {}

  idFromName(name: string): { toString(): string } {
    return { toString: () => name };
  }

  get(id: { toString(): string }): T {
    const idStr = id.toString();
    let stub = this.stubs.get(idStr);
    if (!stub) {
      stub = this.createStub(idStr);
      this.stubs.set(idStr, stub);
    }
    return stub;
  }
}

/**
 * Create mock environment for tests
 */
export function createMockEnv() {
  return {
    WORKFLOW_DB: new MockDurableObjectNamespace((name) => new MockWorkflowRunDOStub(name)),
    WORKFLOW_INDEX: new MockWorkflowIndex(),
    WORKFLOW_QUEUE: new MockDurableObjectNamespace((name) => new MockQueueCellStub(name)),
    WORKFLOW_STREAMS: new MockDurableObjectNamespace(() => new MockStreamDOStub()),
  };
}

/**
 * Clear all mock data (for test cleanup)
 */
export function clearMockData() {
  durableObjectData.clear();
  kvData.clear();
  recordedEnqueues.length = 0;
}
