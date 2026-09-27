import type {
  ExpireRunStreamsResult,
  ExpireStreamResult,
  FinalizeRunStreamsResult,
} from '../../retention.js';
import {
  MAX_STREAM_CHUNK_BYTES,
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
  /**
   * Storage layout, fixed when the stream is created. Absent: one payload row
   * and one size row per chunk. 2: segment rows that each pack one write's
   * chunks, with a small size row per segment.
   */
  layout?: typeof SEGMENT_LAYOUT;
}

interface SegmentSize {
  /** Chunks packed in the segment. */
  count: number;
  /** Payload bytes of those chunks. */
  bytes: number;
}

interface StreamWaiter {
  resolve(reason: 'change' | 'timeout'): void;
  timer: ReturnType<typeof setTimeout>;
  signal?: AbortSignal;
  onAbort?: () => void;
}

interface RegistryExpiry {
  expiredAt: number;
  deleted: number;
}

const META_KEY = 'meta';
const CHUNK_KEY_PREFIX = 'chunk:';
const CHUNK_SIZE_KEY_PREFIX = 'chunk-size:';
/** Registry keys used when this DO instance acts as a per-run stream index. */
const STREAM_REGISTRY_PREFIX = 'stream:';
const REGISTRY_OWNER_KEY = 'registry:owner';
const REGISTRY_EXPIRED_KEY = 'registry:expired';
/** Each chunk occupies a payload key and a size key, deleted in calls of at most 128 keys. */
const DEFAULT_EXPIRE_CHUNK_LIMIT = 64;
const MAX_EXPIRE_CHUNK_LIMIT = 256;
const DELETE_BATCH_KEYS = 128;
/** Bound storage deletion work as well as item count; one oversized chunk still makes progress. */
const DEFAULT_EXPIRE_BYTE_LIMIT = 16 * 1024 * 1024;
const MAX_EXPIRE_BYTE_LIMIT = DEFAULT_EXPIRE_BYTE_LIMIT;
const MAX_STREAM_INDEX = 0x7fffffff;
const SEGMENT_LAYOUT = 2;
const SEGMENT_KEY_PREFIX = 'seg:';
const SEGMENT_SIZE_KEY_PREFIX = 'segsize:';
/** Payload bytes per segment row, well below the 2 MiB per-value storage limit. */
const MAX_SEGMENT_BYTES = 1024 * 1024;
/** Layout of a stream's first write. Existing streams keep the layout they started with. */
const NEW_STREAM_LAYOUT: StreamMeta['layout'] = undefined;
/** One storage get or put call accepts at most 128 keys. */
const STORAGE_BATCH_KEYS = 128;

function boundedLimit(value: number | undefined, fallback: number, maximum: number): number {
  if (value === undefined || !Number.isFinite(value)) return fallback;
  return Math.max(1, Math.min(maximum, Math.floor(value)));
}

function emptyMeta(): StreamMeta {
  return { count: 0, state: 'open' };
}

function validateMeta(meta: unknown): StreamMeta {
  if (!meta || typeof meta !== 'object' || Array.isArray(meta)) {
    throw new Error('Invalid persisted stream metadata');
  }
  const candidate = meta as Partial<StreamMeta>;
  if (
    !Number.isSafeInteger(candidate.count) ||
    candidate.count! < 0 ||
    candidate.count! > MAX_STREAM_INDEX ||
    !['open', 'closed', 'errored', 'expired'].includes(candidate.state as StreamTerminalState) ||
    (candidate.ownerRunId !== undefined && typeof candidate.ownerRunId !== 'string') ||
    (candidate.payloadDeleted !== undefined && typeof candidate.payloadDeleted !== 'boolean') ||
    (candidate.layout !== undefined && candidate.layout !== SEGMENT_LAYOUT)
  ) {
    throw new Error('Invalid persisted stream metadata');
  }
  const error = candidate.error;
  if (
    (error !== undefined &&
      (!error ||
        typeof error !== 'object' ||
        typeof error.name !== 'string' ||
        typeof error.message !== 'string')) ||
    (candidate.state === 'errored') !== (error !== undefined)
  ) {
    throw new Error('Invalid persisted stream error metadata');
  }
  return candidate as StreamMeta;
}

/** Zero-padding keeps storage.list() results in stream offset order. */
function chunkKey(index: number): string {
  return `${CHUNK_KEY_PREFIX}${index.toString().padStart(12, '0')}`;
}

function chunkSizeKey(index: number): string {
  return `${CHUNK_SIZE_KEY_PREFIX}${index.toString().padStart(12, '0')}`;
}

function segmentKey(start: number): string {
  return `${SEGMENT_KEY_PREFIX}${start.toString().padStart(12, '0')}`;
}

function segmentSizeKey(start: number): string {
  return `${SEGMENT_SIZE_KEY_PREFIX}${start.toString().padStart(12, '0')}`;
}

function startFromSegmentKey(key: string, prefix: string): number {
  const suffix = key.slice(prefix.length);
  const start = Number(suffix);
  if (
    !key.startsWith(prefix) ||
    !/^\d{12}$/.test(suffix) ||
    !Number.isSafeInteger(start) ||
    start > MAX_STREAM_INDEX
  ) {
    throw new Error(`Invalid persisted stream segment key "${key}"`);
  }
  return start;
}

function validateSegmentSize(value: unknown, start: number): SegmentSize {
  const size = value as Partial<SegmentSize> | undefined;
  if (
    !size ||
    !Number.isSafeInteger(size.count) ||
    size.count! < 1 ||
    !Number.isSafeInteger(size.bytes) ||
    size.bytes! < 0
  ) {
    throw new Error(`Invalid persisted stream segment size at index ${start}`);
  }
  return size as SegmentSize;
}

/** Pack chunks as `[u32 count][u32 length]*count` followed by the payloads. */
function encodeSegment(chunks: Uint8Array[]): Uint8Array {
  const payloadBytes = chunks.reduce((total, chunk) => total + chunk.byteLength, 0);
  const header = 4 + 4 * chunks.length;
  const encoded = new Uint8Array(header + payloadBytes);
  const view = new DataView(encoded.buffer);
  view.setUint32(0, chunks.length);
  let offset = header;
  for (let index = 0; index < chunks.length; index++) {
    view.setUint32(4 + 4 * index, chunks[index].byteLength);
    encoded.set(chunks[index], offset);
    offset += chunks[index].byteLength;
  }
  return encoded;
}

function decodeSegment(value: unknown, start: number, size: SegmentSize): Uint8Array[] {
  if (!(value instanceof Uint8Array) || value.byteLength < 4) {
    throw new Error(`Invalid persisted stream segment at index ${start}`);
  }
  const view = new DataView(value.buffer, value.byteOffset, value.byteLength);
  const count = view.getUint32(0);
  const header = 4 + 4 * count;
  if (count !== size.count || header + size.bytes !== value.byteLength) {
    throw new Error(`Invalid persisted stream segment at index ${start}`);
  }
  const chunks: Uint8Array[] = [];
  let offset = header;
  for (let index = 0; index < count; index++) {
    const length = view.getUint32(4 + 4 * index);
    if (offset + length > value.byteLength) {
      throw new Error(`Invalid persisted stream segment at index ${start}`);
    }
    chunks.push(value.subarray(offset, offset + length));
    offset += length;
  }
  if (offset !== value.byteLength) {
    throw new Error(`Invalid persisted stream segment at index ${start}`);
  }
  return chunks;
}

/** Group one write's chunks into segments of at most MAX_SEGMENT_BYTES payload bytes. */
function packSegments(
  startIndex: number,
  chunks: Uint8Array[],
): Array<{ start: number; chunks: Uint8Array[] }> {
  const segments: Array<{ start: number; chunks: Uint8Array[] }> = [];
  let current: Uint8Array[] = [];
  let bytes = 0;
  let start = startIndex;
  for (const chunk of chunks) {
    if (current.length > 0 && bytes + chunk.byteLength > MAX_SEGMENT_BYTES) {
      segments.push({ start, chunks: current });
      start += current.length;
      current = [];
      bytes = 0;
    }
    current.push(chunk);
    bytes += chunk.byteLength;
  }
  if (current.length > 0) segments.push({ start, chunks: current });
  return segments;
}

function indexFromChunkSizeKey(key: string): number {
  const suffix = key.slice(CHUNK_SIZE_KEY_PREFIX.length);
  const index = Number(suffix);
  if (
    !/^\d{12}$/.test(suffix) ||
    !Number.isSafeInteger(index) ||
    index < 0 ||
    index > MAX_STREAM_INDEX
  ) {
    throw new Error(`Invalid persisted stream chunk size key "${key}"`);
  }
  return index;
}

function indexFromChunkKey(key: string): number {
  const suffix = key.slice(CHUNK_KEY_PREFIX.length);
  const index = Number(suffix);
  if (
    !/^\d{12}$/.test(suffix) ||
    !Number.isSafeInteger(index) ||
    index < 0 ||
    index > MAX_STREAM_INDEX
  ) {
    throw new Error(`Invalid persisted stream chunk key "${key}"`);
  }
  return index;
}

function validateChunkSize(size: unknown, index: number): number {
  if (
    typeof size !== 'number' ||
    !Number.isSafeInteger(size) ||
    size < 0 ||
    size > MAX_STREAM_CHUNK_BYTES
  ) {
    throw new Error(`Invalid persisted stream chunk size at index ${index}`);
  }
  return size;
}

async function deleteInBatches(txn: DurableObjectTransaction, keys: string[]): Promise<void> {
  for (let offset = 0; offset < keys.length; offset += DELETE_BATCH_KEYS) {
    await txn.delete(keys.slice(offset, offset + DELETE_BATCH_KEYS));
  }
}

function compactChunk(chunk: Uint8Array): Uint8Array {
  return chunk.byteOffset === 0 && chunk.byteLength === chunk.buffer.byteLength
    ? chunk
    : new Uint8Array(chunk);
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

  /** Multi-key read split into calls within the per-call key limit. */
  private async getMany<T>(keys: string[]): Promise<Map<string, T>> {
    if (keys.length <= STORAGE_BATCH_KEYS) return await this.ctx.storage.get<T>(keys);
    const groups: string[][] = [];
    for (let offset = 0; offset < keys.length; offset += STORAGE_BATCH_KEYS) {
      groups.push(keys.slice(offset, offset + STORAGE_BATCH_KEYS));
    }
    const merged = new Map<string, T>();
    for (const result of await Promise.all(groups.map((group) => this.ctx.storage.get<T>(group)))) {
      for (const [key, value] of result) merged.set(key, value);
    }
    return merged;
  }

  private async getMeta(): Promise<StreamMeta> {
    if (this.meta) return this.meta;
    this.metaLoad ??= this.ctx.storage.get<StreamMeta>(META_KEY).then((meta) => {
      const loaded = meta === undefined ? emptyMeta() : validateMeta(meta);
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
    let chunks: Uint8Array[] = [];
    if (available > 0) {
      chunks =
        meta.layout === SEGMENT_LAYOUT
          ? await this.readSegmentRows(request.startIndex, available, request.maxBytes)
          : await this.readChunkRows(request.startIndex, available, request.maxBytes);
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

  private async readChunkRows(
    startIndex: number,
    available: number,
    maxBytes: number,
  ): Promise<Uint8Array[]> {
    const indexes = Array.from({ length: available }, (_, offset) => startIndex + offset);
    const sizeKeys = indexes.map(chunkSizeKey);
    const storedSizes = await this.getMany<number>(sizeKeys);
    const selectedIndexes: number[] = [];
    const selectedSizes: number[] = [];
    let bytes = 0;
    for (const index of indexes) {
      const size = validateChunkSize(storedSizes.get(chunkSizeKey(index)), index);
      if (bytes + size > maxBytes) break;
      selectedIndexes.push(index);
      selectedSizes.push(size);
      bytes += size;
    }

    const payloadKeys = selectedIndexes.map(chunkKey);
    const storedChunks = await this.getMany<Uint8Array>(payloadKeys);
    const chunks: Uint8Array[] = [];
    for (let offset = 0; offset < payloadKeys.length; offset++) {
      const chunk = storedChunks.get(payloadKeys[offset]);
      if (!(chunk instanceof Uint8Array) || chunk.byteLength !== selectedSizes[offset]) {
        throw new Error(`Invalid persisted stream chunk at index ${selectedIndexes[offset]}`);
      }
      chunks.push(chunk);
    }
    return chunks;
  }

  /**
   * Plan from the small segment size rows, then load only the segments that
   * cover the requested range. A read that starts on a segment boundary (the
   * common sequential case) needs one list and one multi-get.
   */
  private async readSegmentRows(
    startIndex: number,
    available: number,
    maxBytes: number,
  ): Promise<Uint8Array[]> {
    const endIndex = startIndex + available;
    const listed = await this.ctx.storage.list<SegmentSize>({
      prefix: SEGMENT_SIZE_KEY_PREFIX,
      start: segmentSizeKey(startIndex),
      end: segmentSizeKey(endIndex),
      limit: available,
    });
    const planned: Array<{ start: number; size: SegmentSize }> = [];
    for (const [key, value] of listed) {
      const start = startFromSegmentKey(key, SEGMENT_SIZE_KEY_PREFIX);
      planned.push({ start, size: validateSegmentSize(value, start) });
    }
    if (planned[0]?.start !== startIndex) {
      // The read starts inside a segment: find the segment that contains it.
      const containing = await this.ctx.storage.list<SegmentSize>({
        prefix: SEGMENT_SIZE_KEY_PREFIX,
        end: segmentSizeKey(startIndex + 1),
        reverse: true,
        limit: 1,
      });
      const entry = containing.entries().next().value;
      if (!entry) throw new Error(`Missing persisted stream segment at index ${startIndex}`);
      const start = startFromSegmentKey(entry[0], SEGMENT_SIZE_KEY_PREFIX);
      planned.unshift({ start, size: validateSegmentSize(entry[1], start) });
    }

    // Load segments until the byte budget is reached; per-chunk sizes are
    // known only after decoding, so the final selection below trims exactly.
    const selected: Array<{ start: number; size: SegmentSize }> = [];
    let plannedBytes = 0;
    for (const segment of planned) {
      if (selected.length > 0 && plannedBytes >= maxBytes) break;
      selected.push(segment);
      plannedBytes += segment.size.bytes;
    }
    const values = await this.getMany<Uint8Array>(selected.map(({ start }) => segmentKey(start)));

    const chunks: Uint8Array[] = [];
    let bytes = 0;
    let expected = startIndex;
    for (const { start, size } of selected) {
      if (start > expected)
        throw new Error(`Missing persisted stream segment at index ${expected}`);
      const segment = decodeSegment(values.get(segmentKey(start)), start, size);
      for (let offset = expected - start; offset < segment.length; offset++) {
        if (expected >= endIndex || bytes + segment[offset].byteLength > maxBytes) return chunks;
        chunks.push(segment[offset]);
        bytes += segment[offset].byteLength;
        expected++;
      }
    }
    return chunks;
  }

  /** Append one bounded binary batch with contiguous indexes. */
  async writeChunks(runId: string, chunks: Uint8Array[]): Promise<StreamWriteResult> {
    validateStreamWriteChunks(chunks);
    return await this.runMutation(async () => {
      const committed = await this.ctx.storage.transaction(async (txn) => {
        const stored = await txn.get<StreamMeta>(META_KEY);
        const meta = stored === undefined ? emptyMeta() : validateMeta(stored);
        this.assertWritable(meta, runId);
        const startIndex = meta.count;
        if (startIndex + chunks.length > MAX_STREAM_INDEX) {
          throw new Error('Stream offset limit exceeded');
        }
        // A stream keeps the layout it was created with.
        const layout = stored === undefined ? NEW_STREAM_LAYOUT : meta.layout;
        const nextMeta: StreamMeta = {
          count: startIndex + chunks.length,
          state: 'open',
          ownerRunId: runId,
          ...(layout === undefined ? {} : { layout }),
        };
        const entries: Array<[string, StreamMeta | Uint8Array | number | SegmentSize]> = [
          [META_KEY, nextMeta],
        ];
        if (layout === SEGMENT_LAYOUT) {
          for (const segment of packSegments(startIndex, chunks)) {
            entries.push([segmentKey(segment.start), encodeSegment(segment.chunks)]);
            entries.push([
              segmentSizeKey(segment.start),
              {
                count: segment.chunks.length,
                bytes: segment.chunks.reduce((total, chunk) => total + chunk.byteLength, 0),
              },
            ]);
          }
        } else {
          const storedChunks = chunks.map(compactChunk);
          for (let offset = 0; offset < storedChunks.length; offset++) {
            const index = startIndex + offset;
            entries.push([chunkKey(index), storedChunks[offset]]);
            entries.push([chunkSizeKey(index), storedChunks[offset].byteLength]);
          }
        }
        // One transaction, written in groups within the per-call key limit.
        for (let offset = 0; offset < entries.length; offset += STORAGE_BATCH_KEYS) {
          await txn.put(Object.fromEntries(entries.slice(offset, offset + STORAGE_BATCH_KEYS)));
        }
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
        const meta = stored === undefined ? emptyMeta() : validateMeta(stored);
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
        const meta = stored === undefined ? emptyMeta() : validateMeta(stored);
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
  async expireRegistry(
    runId: string,
    expiredAt: number,
    options?: { limit?: number },
  ): Promise<ExpireRunStreamsResult> {
    const limit = boundedLimit(options?.limit, 16, 128);
    return await this.ctx.storage.transaction(async (txn) => {
      const state = await txn.get<string | RegistryExpiry>([
        REGISTRY_OWNER_KEY,
        REGISTRY_EXPIRED_KEY,
      ]);
      const owner = state.get(REGISTRY_OWNER_KEY) as string | undefined;
      if (owner !== undefined && owner !== runId) {
        throw new Error(`Stream registry is owned by workflow run "${owner}"`);
      }
      const expiry = state.get(REGISTRY_EXPIRED_KEY) as RegistryExpiry | undefined;
      await txn.put({
        [REGISTRY_OWNER_KEY]: runId,
        [REGISTRY_EXPIRED_KEY]: expiry ?? { expiredAt, deleted: 0 },
      });
      const entries = await txn.list<boolean>({ prefix: STREAM_REGISTRY_PREFIX, limit });
      return {
        streams: Array.from(entries.keys()).map((key) => key.slice(STREAM_REGISTRY_PREFIX.length)),
      };
    });
  }

  /** Remove completed registry entries in a bounded, retry-safe batch. */
  async finalizeRegistry(runId: string, streams: string[]): Promise<FinalizeRunStreamsResult> {
    return await this.ctx.storage.transaction(async (txn) => {
      const state = await txn.get<string | RegistryExpiry>([
        REGISTRY_OWNER_KEY,
        REGISTRY_EXPIRED_KEY,
      ]);
      const owner = state.get(REGISTRY_OWNER_KEY) as string | undefined;
      const expiry = state.get(REGISTRY_EXPIRED_KEY) as RegistryExpiry | undefined;
      if (owner !== runId || expiry === undefined) {
        throw new Error(`Stream registry for workflow run "${runId}" is not expired`);
      }
      const keys = Array.from(new Set(streams.map((name) => `${STREAM_REGISTRY_PREFIX}${name}`)));
      const removed = keys.length === 0 ? 0 : await txn.delete(keys);
      const deleted = expiry.deleted + removed;
      await txn.put<RegistryExpiry>(REGISTRY_EXPIRED_KEY, { ...expiry, deleted });
      const remaining = await txn.list({ prefix: STREAM_REGISTRY_PREFIX, limit: 1 });
      return { deleted, done: remaining.size === 0 };
    });
  }

  /** Delete one page of per-chunk rows, listing size rows to avoid loading payloads. */
  private async expireChunkPage(
    txn: DurableObjectTransaction,
    limit: number,
    byteLimit: number,
  ): Promise<{ chunks: number; bytes: number; done: boolean }> {
    const candidates = await txn.list<number>({
      prefix: CHUNK_SIZE_KEY_PREFIX,
      limit: limit + 1,
    });
    const page: Array<{ index: number; size: number }> = [];
    let bytes = 0;
    for (const [key, storedSize] of Array.from(candidates).slice(0, limit)) {
      const index = indexFromChunkSizeKey(key);
      const size = validateChunkSize(storedSize, index);
      if (page.length > 0 && bytes + size > byteLimit) break;
      page.push({ index, size });
      bytes += size;
    }

    let payloadFallback = false;
    if (candidates.size === 0) {
      const payloads = await txn.list({ prefix: CHUNK_KEY_PREFIX, limit: 1 });
      const first = payloads.entries().next().value;
      if (first) {
        const [key, payload] = first;
        page.push({
          index: indexFromChunkKey(key),
          size: payload instanceof Uint8Array ? payload.byteLength : 0,
        });
        bytes = page[0].size;
        payloadFallback = true;
      }
    }

    const keys = page.flatMap(({ index }) => [chunkKey(index), chunkSizeKey(index)]);
    await deleteInBatches(txn, keys);
    let done = candidates.size === page.length;
    if (payloadFallback) {
      const remaining = await txn.list({ prefix: CHUNK_KEY_PREFIX, limit: 1 });
      done = remaining.size === 0;
    } else if (done && candidates.size > 0) {
      // A corrupt/missing size index can otherwise make cleanup report
      // completion while leaving an orphan payload. Verify once after the
      // final staged size-index page; healthy pages return an empty list.
      const remaining = await txn.list({ prefix: CHUNK_KEY_PREFIX, limit: 1 });
      done = remaining.size === 0;
    }
    return { chunks: page.length, bytes, done };
  }

  /**
   * Delete one page of segment rows, listing segment size rows to avoid
   * loading payloads. One segment always makes progress, even when it holds
   * more chunks or bytes than the page allows.
   */
  private async expireSegmentPage(
    txn: DurableObjectTransaction,
    limit: number,
    byteLimit: number,
  ): Promise<{ chunks: number; bytes: number; done: boolean }> {
    const candidates = await txn.list<SegmentSize>({
      prefix: SEGMENT_SIZE_KEY_PREFIX,
      limit: limit + 1,
    });
    const page: number[] = [];
    let chunks = 0;
    let bytes = 0;
    for (const [key, value] of candidates) {
      const start = startFromSegmentKey(key, SEGMENT_SIZE_KEY_PREFIX);
      const size = validateSegmentSize(value, start);
      if (page.length > 0 && (chunks + size.count > limit || bytes + size.bytes > byteLimit)) {
        break;
      }
      page.push(start);
      chunks += size.count;
      bytes += size.bytes;
    }

    let orphan = false;
    if (candidates.size === 0) {
      // A segment row without its size row: delete it on its own.
      const segments = await txn.list<Uint8Array>({ prefix: SEGMENT_KEY_PREFIX, limit: 1 });
      const first = segments.entries().next().value;
      if (first) {
        page.push(startFromSegmentKey(first[0], SEGMENT_KEY_PREFIX));
        bytes = first[1] instanceof Uint8Array ? first[1].byteLength : 0;
        chunks = 1;
        orphan = true;
      }
    }

    await deleteInBatches(
      txn,
      page.flatMap((start) => [segmentKey(start), segmentSizeKey(start)]),
    );
    let done = !orphan && candidates.size === page.length;
    if (orphan || done) {
      const remaining = await txn.list({ prefix: SEGMENT_KEY_PREFIX, limit: 1 });
      done = remaining.size === 0;
    }
    return { chunks, bytes, done };
  }

  /**
   * Fence the stream and delete one bounded page. Listing size keys avoids
   * loading payloads, and the transaction makes a crash retry resume safely.
   */
  async expireStream(
    runId: string,
    expiredAt: number,
    options?: { limit?: number; byteLimit?: number },
  ): Promise<ExpireStreamResult> {
    const limit = boundedLimit(options?.limit, DEFAULT_EXPIRE_CHUNK_LIMIT, MAX_EXPIRE_CHUNK_LIMIT);
    const byteLimit = boundedLimit(
      options?.byteLimit,
      DEFAULT_EXPIRE_BYTE_LIMIT,
      MAX_EXPIRE_BYTE_LIMIT,
    );
    return await this.runMutation(async () => {
      const result = await this.ctx.storage.transaction(async (txn) => {
        const stored = await txn.get<StreamMeta>(META_KEY);
        const meta = stored === undefined ? emptyMeta() : validateMeta(stored);
        this.assertOwner(meta, runId);
        const firstFence = meta.state !== 'expired';

        const { chunks, bytes, done } =
          meta.layout === SEGMENT_LAYOUT
            ? await this.expireSegmentPage(txn, limit, byteLimit)
            : await this.expireChunkPage(txn, limit, byteLimit);
        if (done && chunks === 0 && meta.state === 'expired' && meta.payloadDeleted) {
          return {
            meta,
            result: { deleted: false, chunks: 0, bytes: 0, done: true },
          };
        }
        const nextMeta: StreamMeta = {
          ...meta,
          count: done ? 0 : meta.count,
          ownerRunId: runId,
          state: 'expired',
          error: undefined,
          expiredAt: meta.expiredAt ?? expiredAt,
          expiredChunkCount: meta.expiredChunkCount ?? meta.count,
          payloadDeleted: done,
        };
        await txn.put(META_KEY, nextMeta);
        return {
          meta: nextMeta,
          result: {
            deleted: firstFence,
            chunks,
            bytes,
            done,
          } satisfies ExpireStreamResult,
        };
      });

      this.meta = result.meta;
      this.metaLoad = Promise.resolve(result.meta);
      this.wakeReaders();
      return result.result;
    });
  }
}
