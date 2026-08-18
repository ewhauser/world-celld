import type {
  GetChunksOptions,
  StreamChunk,
  StreamChunksResponse,
  StreamInfoResponse,
  Streamer,
} from '@workflow/world';
import type {
  ExpireRunStreamsResult,
  ExpireStreamResult,
  FinalizeRunStreamsResult,
} from './retention.js';
import {
  MAX_STREAM_BATCH_BYTES,
  MAX_STREAM_CHUNK_BYTES,
  MAX_STREAM_LONG_POLL_MS,
  MAX_STREAM_READ_BYTES,
  MAX_STREAM_READ_CHUNKS,
  MAX_STREAM_WRITE_CHUNKS,
  normalizeStreamError,
  type StreamErrorData,
  type StreamReadRequest,
  type StreamReadResult,
  type StreamWriteResult,
} from './stream-protocol.js';

export interface CelldStreamerConfig {
  env: {
    WORKFLOW_STREAMS: StreamDONamespace;
  };
  /** Duration of one bounded idle read. Default: 20000. */
  streamLongPollMs?: number;
  /** Runtime coalescing delay for streams.writeMulti(). Default: 0. */
  streamFlushIntervalMs?: number;
}

export interface CelldStreamer extends Omit<Streamer, 'streams'> {
  streams: Streamer['streams'] & {
    fail(runId: string, name: string, error: StreamErrorData | string): Promise<void>;
  };
  writeToStream(
    name: string,
    runId: string | Promise<string>,
    chunk: string | Uint8Array,
  ): Promise<void>;
  writeChunksToStream(
    name: string,
    runId: string | Promise<string>,
    chunks: readonly (string | Uint8Array)[],
  ): Promise<void>;
  closeStream(name: string, runId: string | Promise<string>): Promise<void>;
  errorStream(
    name: string,
    runId: string | Promise<string>,
    error: StreamErrorData | string,
  ): Promise<void>;
  readFromStream(
    name: string,
    runId: string,
    startIndex?: number,
  ): Promise<ReadableStream<Uint8Array>>;
  listStreamsByRunId(runId: string): Promise<string[]>;
  getStreamChunks(
    name: string,
    runId: string,
    options?: GetChunksOptions,
  ): Promise<StreamChunksResponse>;
  getStreamInfo(name: string, runId: string): Promise<StreamInfoResponse>;
}

/** Direct StreamDO RPC surface. Remote stubs specialize read/write as binary HTTP. */
export interface StreamDOStub {
  writeChunks(runId: string, chunks: Uint8Array[]): Promise<StreamWriteResult>;
  readChunks(request: StreamReadRequest, signal?: AbortSignal): Promise<StreamReadResult>;
  closeStream(runId: string): Promise<void>;
  failStream(runId: string, error: StreamErrorData | string): Promise<void>;
  registerStream(runId: string, name: string): Promise<void>;
  listStreams(): Promise<string[]>;
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

export interface StreamDONamespace {
  idFromName(name: string): StreamDOId;
  get(id: StreamDOId): StreamDOStub;
}

export interface StreamDOId {
  toString(): string;
}

const DEFAULT_LONG_POLL_MS = MAX_STREAM_LONG_POLL_MS;
const DEFAULT_CHUNK_PAGE_SIZE = MAX_STREAM_READ_CHUNKS;
const MAX_CHUNK_PAGE_SIZE = MAX_STREAM_READ_CHUNKS;
const REGISTERED_STREAM_CACHE_SIZE = 4096;

function toBytes(chunk: string | Uint8Array): Uint8Array {
  return typeof chunk === 'string' ? new TextEncoder().encode(chunk) : chunk;
}

function throwTerminal(result: StreamReadResult): void {
  if (result.state === 'expired') throw new Error('Stream has expired');
  if (result.state === 'errored') {
    const error = new Error(result.error?.message ?? 'Stream failed');
    error.name = result.error?.name ?? 'Error';
    throw error;
  }
}

export function createStreamer(config: CelldStreamerConfig): CelldStreamer {
  const { env } = config;
  const streamLongPollMs = config.streamLongPollMs ?? DEFAULT_LONG_POLL_MS;
  if (
    !Number.isSafeInteger(streamLongPollMs) ||
    streamLongPollMs < 1 ||
    streamLongPollMs > MAX_STREAM_LONG_POLL_MS
  ) {
    throw new Error(`streamLongPollMs must be between 1 and ${MAX_STREAM_LONG_POLL_MS}`);
  }
  const streamFlushIntervalMs = config.streamFlushIntervalMs ?? 0;
  if (!Number.isSafeInteger(streamFlushIntervalMs) || streamFlushIntervalMs < 0) {
    throw new Error('streamFlushIntervalMs must be a non-negative safe integer');
  }

  const getStreamDO = (streamName: string): StreamDOStub => {
    const id = env.WORKFLOW_STREAMS.idFromName(`stream:${streamName}`);
    return env.WORKFLOW_STREAMS.get(id);
  };

  const getRunRegistryDO = (runId: string): StreamDOStub => {
    const id = env.WORKFLOW_STREAMS.idFromName(`run-streams:${runId}`);
    return env.WORKFLOW_STREAMS.get(id);
  };

  /** Bounded per-isolate cache; eviction only repeats idempotent registration. */
  const registeredStreams = new Set<string>();

  async function registerStreamForRun(name: string, runId: string): Promise<void> {
    const cacheKey = `${runId}\0${name}`;
    if (registeredStreams.has(cacheKey)) return;
    await getRunRegistryDO(runId).registerStream(runId, name);
    if (registeredStreams.size >= REGISTERED_STREAM_CACHE_SIZE) {
      const oldest = registeredStreams.values().next().value;
      if (oldest !== undefined) registeredStreams.delete(oldest);
    }
    registeredStreams.add(cacheKey);
  }

  const writeChunksToStream = async (
    name: string,
    runId: string | Promise<string>,
    input: readonly (string | Uint8Array)[],
  ): Promise<void> => {
    if (input.length === 0) return;
    const resolvedRunId = await runId;
    const chunks = input.map(toBytes);
    for (const chunk of chunks) {
      if (chunk.byteLength > MAX_STREAM_CHUNK_BYTES) {
        throw new Error(`Stream chunk exceeds ${MAX_STREAM_CHUNK_BYTES} bytes`);
      }
    }
    await registerStreamForRun(name, resolvedRunId);

    let batch: Uint8Array[] = [];
    let batchBytes = 0;
    for (const chunk of chunks) {
      if (
        batch.length > 0 &&
        (batch.length === MAX_STREAM_WRITE_CHUNKS ||
          batchBytes + chunk.byteLength > MAX_STREAM_BATCH_BYTES)
      ) {
        await getStreamDO(name).writeChunks(resolvedRunId, batch);
        batch = [];
        batchBytes = 0;
      }
      batch.push(chunk);
      batchBytes += chunk.byteLength;
    }
    if (batch.length > 0) await getStreamDO(name).writeChunks(resolvedRunId, batch);
  };

  const writeToStream = (
    name: string,
    runId: string | Promise<string>,
    chunk: string | Uint8Array,
  ): Promise<void> => writeChunksToStream(name, runId, [chunk]);

  const closeStream = async (name: string, runId: string | Promise<string>): Promise<void> => {
    const resolvedRunId = await runId;
    await registerStreamForRun(name, resolvedRunId);
    await getStreamDO(name).closeStream(resolvedRunId);
  };

  const errorStream = async (
    name: string,
    runId: string | Promise<string>,
    error: StreamErrorData | string,
  ): Promise<void> => {
    const resolvedRunId = await runId;
    await registerStreamForRun(name, resolvedRunId);
    await getStreamDO(name).failStream(resolvedRunId, normalizeStreamError(error));
  };

  const readFromStream = async (
    name: string,
    runId: string,
    startIndex = 0,
  ): Promise<ReadableStream<Uint8Array>> => {
    const stub = getStreamDO(name);

    let nextIndex: number;
    if (startIndex < 0) {
      const info = await stub.readChunks({
        runId,
        startIndex: 0,
        maxChunks: 0,
        maxBytes: MAX_STREAM_READ_BYTES,
        waitMs: 0,
      });
      throwTerminal(info);
      nextIndex = Math.max(0, info.tailIndex + 1 + startIndex);
    } else {
      nextIndex = startIndex;
    }

    let cancelled = false;
    const abortController = new AbortController();

    return new ReadableStream<Uint8Array>({
      async pull(controller) {
        for (;;) {
          if (cancelled) return;
          let result: StreamReadResult;
          try {
            result = await stub.readChunks(
              {
                runId,
                startIndex: nextIndex,
                maxChunks: MAX_STREAM_READ_CHUNKS,
                maxBytes: MAX_STREAM_READ_BYTES,
                waitMs: streamLongPollMs,
              },
              abortController.signal,
            );
          } catch (error) {
            if (cancelled || abortController.signal.aborted) return;
            throw error;
          }

          if (result.chunks.length > 0) {
            for (const chunk of result.chunks) controller.enqueue(chunk);
            nextIndex += result.chunks.length;
            return;
          }
          throwTerminal(result);
          if (result.state === 'closed') {
            controller.close();
            return;
          }
          // A normal long-poll timeout returns no data; keep the pending
          // ReadableStream read attached through the next bounded request.
        }
      },

      cancel() {
        cancelled = true;
        abortController.abort(new DOMException('Stream reader cancelled', 'AbortError'));
      },
    });
  };

  const listStreamsByRunId = (runId: string): Promise<string[]> =>
    getRunRegistryDO(runId).listStreams();

  const getStreamChunks = async (
    name: string,
    runId: string,
    options?: GetChunksOptions,
  ): Promise<StreamChunksResponse> => {
    const requestedLimit = options?.limit ?? DEFAULT_CHUNK_PAGE_SIZE;
    if (!Number.isSafeInteger(requestedLimit) || requestedLimit < 1) {
      throw new Error('Stream chunk limit must be a positive safe integer');
    }
    const limit = Math.min(requestedLimit, MAX_CHUNK_PAGE_SIZE);
    const startIndex = options?.cursor ? Number.parseInt(options.cursor, 10) : 0;
    if (Number.isNaN(startIndex) || startIndex < 0) {
      throw new Error(`Invalid stream cursor: ${options?.cursor}`);
    }

    const result = await getStreamDO(name).readChunks({
      runId,
      startIndex,
      maxChunks: limit,
      maxBytes: MAX_STREAM_READ_BYTES,
      waitMs: 0,
    });
    throwTerminal(result);
    const data: StreamChunk[] = result.chunks.map((chunk, offset) => ({
      index: startIndex + offset,
      data: chunk,
    }));
    const nextIndex = startIndex + data.length;
    const hasMore = nextIndex <= result.tailIndex;

    return {
      data,
      cursor: hasMore ? String(nextIndex) : null,
      hasMore,
      done: result.state === 'closed',
    };
  };

  const getStreamInfo = async (name: string, runId: string): Promise<StreamInfoResponse> => {
    const result = await getStreamDO(name).readChunks({
      runId,
      startIndex: 0,
      maxChunks: 0,
      maxBytes: MAX_STREAM_READ_BYTES,
      waitMs: 0,
    });
    throwTerminal(result);
    return { tailIndex: result.tailIndex, done: result.state === 'closed' };
  };

  return {
    streamFlushIntervalMs,
    streams: {
      write: (runId, name, chunk) => writeToStream(name, runId, chunk),
      writeMulti: (runId, name, chunks) => writeChunksToStream(name, runId, chunks),
      close: (runId, name) => closeStream(name, runId),
      fail: (runId, name, error) => errorStream(name, runId, error),
      get: (runId, name, startIndex) => readFromStream(name, runId, startIndex),
      list: listStreamsByRunId,
      getChunks: (runId, name, options) => getStreamChunks(name, runId, options),
      getInfo: (runId, name) => getStreamInfo(name, runId),
    },
    writeToStream,
    writeChunksToStream,
    closeStream,
    errorStream,
    readFromStream,
    listStreamsByRunId,
    getStreamChunks,
    getStreamInfo,
  };
}
