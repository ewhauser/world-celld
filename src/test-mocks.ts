/**
 * Test mocks for Node.js environment.
 *
 * The mock Durable Object stubs delegate to the SAME transactional
 * event-application core (`apply-event.ts`) as the real WorkflowRunDO, so
 * unit tests exercise the real guard/idempotency/pagination logic — only the
 * storage medium (an in-memory sorted map) is mocked.
 */

import { slotToEventId, type Event, type Hook, type Step, type WorkflowRun } from '@workflow/world';
import type { HookTokenOwner } from './config.js';
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
import type {
  CleanupRecord,
  ExpireRunIndexesRequest,
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
 * Mock KV Namespace with real cursor pagination semantics: the cursor
 * continues after the last key returned by the previous list call, and
 * `list_complete` reflects whether further keys exist.
 */
class MockKVNamespace {
  async get(key: string): Promise<string | null> {
    return kvData.get(key) ?? null;
  }

  async put(key: string, value: string): Promise<void> {
    kvData.set(key, value);
  }

  async putOwned(runId: string, key: string, value: string): Promise<{ stored: boolean }> {
    if (kvData.has(`expired:${runId}`)) return { stored: false };
    kvData.set(key, value);
    return { stored: true };
  }

  async expireRun(request: ExpireRunIndexesRequest): Promise<{ deleted: number }> {
    kvData.set(`expired:${request.runId}`, String(request.expiredAt));
    let deleted = 0;
    for (const key of request.keys) {
      if (kvData.delete(key)) deleted++;
    }
    for (const hook of request.hooks) {
      for (const key of [
        `hook:${hook.token}`,
        `hookid:${hook.hookId}`,
        `hookclaim:${hook.token}`,
      ]) {
        if (kvData.delete(key)) deleted++;
      }
    }
    return { deleted };
  }

  async delete(key: string): Promise<void> {
    kvData.delete(key);
  }

  async list(options?: {
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

  async reserveHookToken(
    token: string,
    owner: HookTokenOwner,
  ): Promise<{ claimed: boolean; holder?: HookTokenOwner }> {
    const raw = kvData.get(`hook:${token}`);
    if (raw !== undefined) {
      const hook = parse<Hook>(raw);
      const holder = { runId: hook.runId, hookId: hook.hookId };
      return { claimed: false, holder };
    }

    const claimKey = `hookclaim:${token}`;
    const claim = kvData.get(claimKey);
    if (claim !== undefined) {
      const holder = JSON.parse(claim) as HookTokenOwner;
      return holder.runId === owner.runId && holder.hookId === owner.hookId
        ? { claimed: true }
        : { claimed: false, holder };
    }

    kvData.set(claimKey, JSON.stringify(owner));
    return { claimed: true };
  }

  async finalizeHookIndexes(
    token: string,
    hookId: string,
    serializedHook: string,
    owner: HookTokenOwner,
  ): Promise<void> {
    const raw = kvData.get(`hook:${token}`);
    if (raw !== undefined) {
      const hook = parse<Hook>(raw);
      if (hook.runId !== owner.runId || hook.hookId !== owner.hookId) {
        throw new Error(`Hook token ${token} is owned by another hook`);
      }
    }
    const claim = kvData.get(`hookclaim:${token}`);
    if (claim !== undefined) {
      const holder = JSON.parse(claim) as HookTokenOwner;
      if (holder.runId !== owner.runId || holder.hookId !== owner.hookId) {
        throw new Error(`Hook token ${token} is reserved by another hook`);
      }
    }
    kvData.set(`hook:${token}`, serializedHook);
    kvData.set(`hookid:${hookId}`, serializedHook);
    kvData.delete(`hookclaim:${token}`);
  }

  async releaseHookToken(token: string, owner: HookTokenOwner): Promise<void> {
    const key = `hookclaim:${token}`;
    const claim = kvData.get(key);
    if (claim !== undefined) {
      const holder = JSON.parse(claim) as HookTokenOwner;
      if (holder.runId === owner.runId && holder.hookId === owner.hookId) {
        kvData.delete(key);
      }
    }
  }

  async deleteHookIndexes(token: string, hookId: string, owner: HookTokenOwner): Promise<void> {
    for (const key of [`hook:${token}`, `hookid:${hookId}`]) {
      const raw = kvData.get(key);
      if (raw !== undefined) {
        const hook = parse<Hook>(raw);
        if (hook.runId === owner.runId && hook.hookId === owner.hookId) {
          kvData.delete(key);
        }
      }
    }
    await this.releaseHookToken(token, owner);
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

  async expireRegistry(runId: string): Promise<{ streams: string[] }> {
    if (this.ownerRunId && this.ownerRunId !== runId) throw new Error('Registry owner mismatch');
    this.ownerRunId = runId;
    this.expired = true;
    return { streams: Array.from(this.registry) };
  }

  async finalizeRegistry(runId: string): Promise<void> {
    if (this.ownerRunId !== runId || !this.expired) throw new Error('Registry is not expired');
    this.registry.clear();
  }

  async expireStream(runId: string): Promise<{ deleted: boolean; chunks: number }> {
    if (this.ownerRunId && this.ownerRunId !== runId) throw new Error('Stream owner mismatch');
    const chunks = this.chunks.length;
    this.ownerRunId = runId;
    this.chunks = [];
    this.state = 'expired';
    this.expired = true;
    this.wake();
    return { deleted: true, chunks };
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

  async expireRun(): Promise<{ deleted: number }> {
    return { deleted: 0 };
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
    WORKFLOW_INDEX: new MockKVNamespace(),
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
