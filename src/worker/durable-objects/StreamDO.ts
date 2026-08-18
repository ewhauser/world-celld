import type { ExpireRunStreamsResult, ExpireStreamResult } from '../../retention.js';
import {
  normalizeStreamError,
  validateStreamReadRequest,
  validateStreamWriteChunks,
  type StreamErrorData,
  type StreamReadRequest,
  type StreamReadResult,
  type StreamTerminalState,
  type StreamWriteResult,
} from '../../stream-protocol.js';
import { DurableObject } from '../do-base.js';

interface StreamMeta {
  /** Number of durable chunks (also the next chunk index). */
  count: number;
  state: StreamTerminalState;
  ownerRunId?: string;
  error?: StreamErrorData;
  expiredAt?: number;
  expiredChunkCount?: number;
  payloadDeleted?: boolean;
}

interface StreamWaiter {
  resolve(reason: 'change' | 'timeout'): void;
  timer: ReturnType<typeof setTimeout>;
  signal?: AbortSignal;
  onAbort?: () => void;
}

const META_KEY = 'meta';
const CHUNK_KEY_PREFIX = 'chunk:';
/** Registry keys used when this DO instance acts as a per-run stream index. */
const STREAM_REGISTRY_PREFIX = 'stream:';
const REGISTRY_OWNER_KEY = 'registry:owner';
const REGISTRY_EXPIRED_KEY = 'registry:expired';
const RETENTION_DELETE_BATCH = 128;

function emptyMeta(): StreamMeta {
  return { count: 0, state: 'open' };
}

function validateMeta(meta: StreamMeta): StreamMeta {
  if (
    !Number.isSafeInteger(meta.count) ||
    meta.count < 0 ||
    meta.count > 0x7fffffff ||
    !['open', 'closed', 'errored', 'expired'].includes(meta.state)
  ) {
    throw new Error('Invalid persisted stream metadata');
  }
  if (meta.state === 'errored' && !meta.error) {
    throw new Error('Invalid persisted stream error metadata');
  }
  return meta;
}

/** Zero-padding keeps storage.list() results in stream offset order. */
function chunkKey(index: number): string {
  return `${CHUNK_KEY_PREFIX}${index.toString().padStart(12, '0')}`;
}

function abortReason(signal: AbortSignal): unknown {
  return signal.reason ?? new DOMException('The operation was aborted', 'AbortError');
}

/**
 * Durable Object backing workflow streams.
 *
 * Stream cells persist binary chunks under monotonic offset keys. Public
 * reads are bounded long polls, and writes append a bounded ordered batch in
 * one storage transaction. Per-run registry cells retain their existing role.
 */
export class StreamDO extends DurableObject {
  private meta: StreamMeta | undefined;
  private metaLoad: Promise<StreamMeta> | undefined;
  private mutationTail: Promise<void> = Promise.resolve();
  private changeVersion = 0;
  private readonly waiters = new Set<StreamWaiter>();

  private async getMeta(): Promise<StreamMeta> {
    if (this.meta) return this.meta;
    this.metaLoad ??= this.ctx.storage.get<StreamMeta>(META_KEY).then((meta) => {
      const loaded = meta ? validateMeta(meta) : emptyMeta();
      this.meta = loaded;
      return loaded;
    });
    try {
      return await this.metaLoad;
    } catch (error) {
      this.metaLoad = undefined;
      throw error;
    }
  }

  private assertOwner(meta: StreamMeta, runId: string): void {
    if (!runId) throw new Error('Stream runId is required');
    if (meta.ownerRunId !== undefined && meta.ownerRunId !== runId) {
      throw new Error(`Stream is owned by workflow run "${meta.ownerRunId}"`);
    }
  }

  private assertWritable(meta: StreamMeta, runId: string): void {
    this.assertOwner(meta, runId);
    if (meta.state === 'expired') throw new Error(`Workflow run "${runId}" has expired`);
    if (meta.state === 'closed') throw new Error('Cannot write to a closed stream');
    if (meta.state === 'errored') throw new Error('Cannot write to an errored stream');
  }

  private runMutation<T>(operation: () => Promise<T>): Promise<T> {
    const result = this.mutationTail.then(operation, operation);
    this.mutationTail = result.then(
      () => undefined,
      () => undefined,
    );
    return result;
  }

  private wakeReaders(): void {
    this.changeVersion += 1;
    for (const waiter of Array.from(this.waiters)) waiter.resolve('change');
  }

  private waitForChange(
    observedVersion: number,
    waitMs: number,
    signal?: AbortSignal,
  ): Promise<'change' | 'timeout'> {
    if (signal?.aborted) return Promise.reject(abortReason(signal));

    return new Promise<'change' | 'timeout'>((resolve, reject) => {
      let settled = false;
      const waiter: StreamWaiter = {
        timer: setTimeout(() => finish('timeout'), waitMs),
        signal,
        resolve: (reason) => finish(reason),
      };

      const cleanup = () => {
        clearTimeout(waiter.timer);
        this.waiters.delete(waiter);
        if (waiter.signal && waiter.onAbort) {
          waiter.signal.removeEventListener('abort', waiter.onAbort);
        }
      };
      const finish = (reason: 'change' | 'timeout') => {
        if (settled) return;
        settled = true;
        cleanup();
        resolve(reason);
      };
      waiter.onAbort = () => {
        if (settled) return;
        settled = true;
        cleanup();
        reject(abortReason(signal!));
      };

      this.waiters.add(waiter);
      signal?.addEventListener('abort', waiter.onAbort, { once: true });

      // A writer may have committed after the caller inspected metadata but
      // before this waiter was installed. The generation check closes that
      // lost-wakeup window without another storage read.
      if (this.changeVersion !== observedVersion) finish('change');
    });
  }

  private async readSnapshot(
    request: StreamReadRequest,
    timedOut: boolean,
  ): Promise<StreamReadResult> {
    const meta = await this.getMeta();
    this.assertOwner(meta, request.runId);

    const available =
      meta.state === 'expired'
        ? 0
        : Math.max(0, Math.min(request.maxChunks, meta.count - request.startIndex));
    const chunks: Uint8Array[] = [];
    if (available > 0) {
      const entries = await this.ctx.storage.list<Uint8Array>({
        prefix: CHUNK_KEY_PREFIX,
        start: chunkKey(request.startIndex),
        limit: available,
      });
      let bytes = 0;
      for (const chunk of entries.values()) {
        if (bytes + chunk.byteLength > request.maxBytes) break;
        chunks.push(chunk);
        bytes += chunk.byteLength;
      }
    }

    return {
      startIndex: request.startIndex,
      tailIndex: meta.count - 1,
      chunks,
      state: meta.state,
      timedOut,
      error: meta.error,
    };
  }

  /** Append one bounded binary batch with contiguous indexes. */
  async writeChunks(runId: string, chunks: Uint8Array[]): Promise<StreamWriteResult> {
    validateStreamWriteChunks(chunks);
    return await this.runMutation(async () => {
      const committed = await this.ctx.storage.transaction(async (txn) => {
        const stored = await txn.get<StreamMeta>(META_KEY);
        const meta = stored ? validateMeta(stored) : emptyMeta();
        this.assertWritable(meta, runId);
        const startIndex = meta.count;
        if (startIndex + chunks.length > 0x7fffffff) {
          throw new Error('Stream offset limit exceeded');
        }
        const nextMeta: StreamMeta = {
          count: startIndex + chunks.length,
          state: 'open',
          ownerRunId: runId,
        };
        const entries: Record<string, StreamMeta | Uint8Array> = { [META_KEY]: nextMeta };
        for (let offset = 0; offset < chunks.length; offset++) {
          entries[chunkKey(startIndex + offset)] = chunks[offset];
        }
        await txn.put(entries);
        return {
          meta: nextMeta,
          result: {
            startIndex,
            count: chunks.length,
            tailIndex: nextMeta.count - 1,
          } satisfies StreamWriteResult,
        };
      });
      this.meta = committed.meta;
      this.metaLoad = Promise.resolve(committed.meta);
      this.wakeReaders();
      return committed.result;
    });
  }

  /**
   * Return available binary chunks and terminal metadata in one bounded
   * operation, waiting at most waitMs when the requested offset is idle.
   */
  async readChunks(request: StreamReadRequest, signal?: AbortSignal): Promise<StreamReadResult> {
    validateStreamReadRequest(request);
    const observedVersion = this.changeVersion;
    const immediate = await this.readSnapshot(request, false);
    if (
      immediate.chunks.length > 0 ||
      immediate.state !== 'open' ||
      request.waitMs === 0 ||
      request.maxChunks === 0
    ) {
      return immediate;
    }

    const reason = await this.waitForChange(observedVersion, request.waitMs, signal);
    return await this.readSnapshot(request, reason === 'timeout');
  }

  /** Close idempotently; the first terminal state wins. */
  async closeStream(runId: string): Promise<void> {
    await this.runMutation(async () => {
      const nextMeta = await this.ctx.storage.transaction(async (txn) => {
        const stored = await txn.get<StreamMeta>(META_KEY);
        const meta = stored ? validateMeta(stored) : emptyMeta();
        this.assertOwner(meta, runId);
        if (meta.state === 'expired') throw new Error(`Workflow run "${runId}" has expired`);
        if (meta.state !== 'open') return meta;
        const closed: StreamMeta = { ...meta, ownerRunId: runId, state: 'closed' };
        await txn.put(META_KEY, closed);
        return closed;
      });
      this.meta = nextMeta;
      this.metaLoad = Promise.resolve(nextMeta);
      this.wakeReaders();
    });
  }

  /** Persist a terminal stream failure and wake every pending reader. */
  async failStream(runId: string, error: StreamErrorData | string): Promise<void> {
    const normalized = normalizeStreamError(error);
    await this.runMutation(async () => {
      const nextMeta = await this.ctx.storage.transaction(async (txn) => {
        const stored = await txn.get<StreamMeta>(META_KEY);
        const meta = stored ? validateMeta(stored) : emptyMeta();
        this.assertOwner(meta, runId);
        if (meta.state === 'expired') throw new Error(`Workflow run "${runId}" has expired`);
        if (meta.state !== 'open') return meta;
        const failed: StreamMeta = {
          ...meta,
          ownerRunId: runId,
          state: 'errored',
          error: normalized,
        };
        await txn.put(META_KEY, failed);
        return failed;
      });
      this.meta = nextMeta;
      this.metaLoad = Promise.resolve(nextMeta);
      this.wakeReaders();
    });
  }

  /** Register a stream name against this per-run registry instance. */
  async registerStream(runId: string, name: string): Promise<void> {
    await this.ctx.storage.transaction(async (txn) => {
      if ((await txn.get(REGISTRY_EXPIRED_KEY)) !== undefined) {
        throw new Error(`Workflow run "${runId}" has expired`);
      }
      const owner = await txn.get<string>(REGISTRY_OWNER_KEY);
      if (owner !== undefined && owner !== runId) {
        throw new Error(`Stream registry is owned by workflow run "${owner}"`);
      }
      await txn.put(REGISTRY_OWNER_KEY, runId);
      await txn.put(`${STREAM_REGISTRY_PREFIX}${name}`, true);
    });
  }

  async listStreams(): Promise<string[]> {
    if ((await this.ctx.storage.get(REGISTRY_EXPIRED_KEY)) !== undefined) {
      throw new Error('Workflow run streams have expired');
    }
    const entries = await this.ctx.storage.list<boolean>({ prefix: STREAM_REGISTRY_PREFIX });
    return Array.from(entries.keys()).map((key) => key.slice(STREAM_REGISTRY_PREFIX.length));
  }

  /** Fence a run's registry before any stream cells are removed. */
  async expireRegistry(runId: string, expiredAt: number): Promise<ExpireRunStreamsResult> {
    return await this.ctx.storage.transaction(async (txn) => {
      const owner = await txn.get<string>(REGISTRY_OWNER_KEY);
      if (owner !== undefined && owner !== runId) {
        throw new Error(`Stream registry is owned by workflow run "${owner}"`);
      }
      await txn.put(REGISTRY_OWNER_KEY, runId);
      await txn.put(REGISTRY_EXPIRED_KEY, expiredAt);
      const entries = await txn.list<boolean>({ prefix: STREAM_REGISTRY_PREFIX });
      return {
        streams: Array.from(entries.keys()).map((key) => key.slice(STREAM_REGISTRY_PREFIX.length)),
      };
    });
  }

  /** Remove the registry payload after every referenced stream was fenced. */
  async finalizeRegistry(runId: string): Promise<void> {
    await this.ctx.storage.transaction(async (txn) => {
      const owner = await txn.get<string>(REGISTRY_OWNER_KEY);
      const expiredAt = await txn.get<number>(REGISTRY_EXPIRED_KEY);
      if (owner !== runId || expiredAt === undefined) {
        throw new Error(`Stream registry for workflow run "${runId}" is not expired`);
      }
      const entries = await txn.list({ prefix: STREAM_REGISTRY_PREFIX });
      const keys = Array.from(entries.keys());
      for (let offset = 0; offset < keys.length; offset += RETENTION_DELETE_BATCH) {
        await txn.delete(keys.slice(offset, offset + RETENTION_DELETE_BATCH));
      }
    });
  }

  /**
   * Fence first, wake readers, then delete known chunk keys in bounded groups.
   * The tombstone records progress so restart/retry remains idempotent.
   */
  async expireStream(runId: string, expiredAt: number): Promise<ExpireStreamResult> {
    return await this.runMutation(async () => {
      const fenced = await this.ctx.storage.transaction(async (txn) => {
        const stored = await txn.get<StreamMeta>(META_KEY);
        const meta = stored ? validateMeta(stored) : emptyMeta();
        this.assertOwner(meta, runId);
        const chunkCount = meta.expiredChunkCount ?? meta.count;
        if (meta.state === 'expired' && meta.payloadDeleted) {
          return { meta, chunkCount, alreadyDeleted: true };
        }
        const nextMeta: StreamMeta = {
          ...meta,
          ownerRunId: runId,
          state: 'expired',
          error: undefined,
          expiredAt,
          expiredChunkCount: chunkCount,
          payloadDeleted: false,
        };
        await txn.put(META_KEY, nextMeta);
        return { meta: nextMeta, chunkCount, alreadyDeleted: false };
      });

      this.meta = fenced.meta;
      this.metaLoad = Promise.resolve(fenced.meta);
      this.wakeReaders();

      if (!fenced.alreadyDeleted) {
        for (let offset = 0; offset < fenced.chunkCount; offset += RETENTION_DELETE_BATCH) {
          const keys: string[] = [];
          const end = Math.min(fenced.chunkCount, offset + RETENTION_DELETE_BATCH);
          for (let index = offset; index < end; index++) keys.push(chunkKey(index));
          await this.ctx.storage.delete(keys);
        }
        const tombstone: StreamMeta = {
          ...fenced.meta,
          count: 0,
          payloadDeleted: true,
        };
        await this.ctx.storage.put(META_KEY, tombstone);
        this.meta = tombstone;
        this.metaLoad = Promise.resolve(tombstone);
      }

      return { deleted: true, chunks: fenced.chunkCount };
    });
  }
}
