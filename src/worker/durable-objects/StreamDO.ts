import { DurableObject } from '../do-base.js';
import type { ExpireRunStreamsResult, ExpireStreamResult } from '../../retention.js';

interface StreamMeta {
  /** Number of chunks written so far (also the next chunk index). */
  count: number;
  /** Whether the stream has been closed (EOF). */
  closed: boolean;
  ownerRunId?: string;
  expiredAt?: number;
}

const META_KEY = 'meta';
const CHUNK_KEY_PREFIX = 'chunk:';
/** Registry keys used when this DO instance acts as a per-run stream index. */
const STREAM_REGISTRY_PREFIX = 'stream:';
const REGISTRY_OWNER_KEY = 'registry:owner';
const REGISTRY_EXPIRED_KEY = 'registry:expired';

/**
 * Zero-pad chunk indexes so `storage.list({ prefix })` returns chunks in
 * write order. 12 digits comfortably exceeds any realistic chunk count.
 */
function chunkKey(index: number): string {
  return `${CHUNK_KEY_PREFIX}${index.toString().padStart(12, '0')}`;
}

/**
 * Durable Object backing workflow streams.
 *
 * Two roles, selected by the DO name:
 * - `stream:<name>`: chunk storage for a single stream. A monotonic
 *   per-stream counter (maintained transactionally inside the DO) assigns
 *   each chunk its index; one chunk per storage key.
 * - `run-streams:<runId>`: registry of stream names owned by a run,
 *   powering `listStreamsByRunId`.
 */
export class StreamDO extends DurableObject {
  private async getMeta(): Promise<StreamMeta> {
    const meta = await this.ctx.storage.get<StreamMeta>(META_KEY);
    return meta ?? { count: 0, closed: false };
  }

  private assertOwner(meta: StreamMeta, runId: string): void {
    if (meta.expiredAt !== undefined) {
      throw new Error(`Workflow run "${runId}" has expired`);
    }
    if (meta.ownerRunId !== undefined && meta.ownerRunId !== runId) {
      throw new Error(`Stream is owned by workflow run "${meta.ownerRunId}"`);
    }
  }

  /**
   * Append a chunk. The index is allocated inside the transaction, so
   * concurrent writers can never collide or skip.
   */
  async writeChunk(runId: string, data: Uint8Array): Promise<number> {
    return await this.ctx.storage.transaction(async (txn) => {
      const meta = (await txn.get<StreamMeta>(META_KEY)) ?? { count: 0, closed: false };
      this.assertOwner(meta, runId);
      if (meta.closed) {
        throw new Error('Cannot write to a closed stream');
      }
      const index = meta.count;
      await txn.put(chunkKey(index), data);
      await txn.put<StreamMeta>(META_KEY, {
        count: index + 1,
        closed: false,
        ownerRunId: runId,
      });
      return index;
    });
  }

  /**
   * Close the stream (idempotent). Readers observe `done: true` once all
   * previously written chunks have been consumed.
   */
  async closeStream(runId: string): Promise<void> {
    await this.ctx.storage.transaction(async (txn) => {
      const meta = (await txn.get<StreamMeta>(META_KEY)) ?? { count: 0, closed: false };
      this.assertOwner(meta, runId);
      await txn.put<StreamMeta>(META_KEY, { ...meta, ownerRunId: runId, closed: true });
    });
  }

  /**
   * Read up to `limit` chunks starting at `startIndex` (0-based, inclusive).
   */
  async getChunks(params: { startIndex: number; limit: number }): Promise<{
    chunks: Uint8Array[];
    /** Whether the stream is closed. */
    done: boolean;
    /** Index of the last written chunk, -1 when empty. */
    tailIndex: number;
  }> {
    const meta = await this.getMeta();
    if (meta.expiredAt !== undefined) {
      throw new Error('Stream has expired');
    }
    const start = Math.max(0, params.startIndex);
    const entries = await this.ctx.storage.list<Uint8Array>({
      prefix: CHUNK_KEY_PREFIX,
      start: chunkKey(start),
      limit: params.limit,
    });
    return {
      chunks: Array.from(entries.values()),
      done: meta.closed,
      tailIndex: meta.count - 1,
    };
  }

  /**
   * Lightweight stream metadata: last chunk index and completion flag.
   */
  async getInfo(): Promise<{ tailIndex: number; done: boolean }> {
    const meta = await this.getMeta();
    if (meta.expiredAt !== undefined) {
      throw new Error('Stream has expired');
    }
    return { tailIndex: meta.count - 1, done: meta.closed };
  }

  /**
   * Register a stream name against this per-run registry instance.
   */
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

  /**
   * List stream names registered against this per-run registry instance.
   */
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
      for (const key of entries.keys()) await txn.delete(key);
    });
  }

  /** Delete stream payload while leaving a small ownership tombstone. */
  async expireStream(runId: string, expiredAt: number): Promise<ExpireStreamResult> {
    return await this.ctx.storage.transaction(async (txn) => {
      const meta = (await txn.get<StreamMeta>(META_KEY)) ?? { count: 0, closed: false };
      if (meta.ownerRunId !== undefined && meta.ownerRunId !== runId) {
        throw new Error(`Stream is owned by workflow run "${meta.ownerRunId}"`);
      }
      const chunks = await txn.list({ prefix: CHUNK_KEY_PREFIX });
      for (const key of chunks.keys()) {
        await txn.delete(key);
      }
      await txn.put<StreamMeta>(META_KEY, {
        count: 0,
        closed: true,
        ownerRunId: runId,
        expiredAt,
      });
      return { deleted: true, chunks: chunks.size };
    });
  }
}
