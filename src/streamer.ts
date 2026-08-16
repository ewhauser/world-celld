import type {
  GetChunksOptions,
  StreamChunk,
  StreamChunksResponse,
  StreamInfoResponse,
  Streamer,
} from '@workflow/world';
import type { ExpireRunStreamsResult, ExpireStreamResult } from './retention.js';

export interface CelldStreamerConfig {
  env: {
    WORKFLOW_STREAMS: StreamDONamespace;
  };
  /**
   * Poll interval (ms) while waiting for new chunks on a live stream.
   * Modified vs upstream: configurable because each poll is a fleet HTTP
   * round-trip here, not an in-process DO call. Default: 100.
   */
  readPollMs?: number;
}

/** Workflow 5 streamer plus the package's pre-5 convenience aliases. */
export interface CelldStreamer extends Streamer {
  writeToStream(
    name: string,
    runId: string | Promise<string>,
    chunk: string | Uint8Array,
  ): Promise<void>;
  closeStream(name: string, runId: string | Promise<string>): Promise<void>;
  readFromStream(name: string, startIndex?: number): Promise<ReadableStream<Uint8Array>>;
  listStreamsByRunId(runId: string): Promise<string[]>;
  getStreamChunks(
    name: string,
    runId: string,
    options?: GetChunksOptions,
  ): Promise<StreamChunksResponse>;
  getStreamInfo(name: string, runId: string): Promise<StreamInfoResponse>;
}

/**
 * RPC surface of StreamDO (see durable-objects/StreamDO.ts). The streamer
 * talks to the DO exclusively via these methods — there is no fetch()
 * protocol.
 */
export interface StreamDOStub {
  writeChunk(runId: string, data: Uint8Array): Promise<number>;
  closeStream(runId: string): Promise<void>;
  getChunks(params: {
    startIndex: number;
    limit: number;
  }): Promise<{ chunks: Uint8Array[]; done: boolean; tailIndex: number }>;
  getInfo(): Promise<{ tailIndex: number; done: boolean }>;
  registerStream(runId: string, name: string): Promise<void>;
  listStreams(): Promise<string[]>;
  expireRegistry(runId: string, expiredAt: number): Promise<ExpireRunStreamsResult>;
  finalizeRegistry(runId: string): Promise<void>;
  expireStream(runId: string, expiredAt: number): Promise<ExpireStreamResult>;
}

export interface StreamDONamespace {
  idFromName(name: string): StreamDOId;
  get(id: StreamDOId): StreamDOStub;
}

export interface StreamDOId {
  toString(): string;
}

/** Default poll interval while waiting for new chunks on a live stream. */
const READ_POLL_MS = 100;
/** Chunks fetched per DO round-trip while reading. */
const READ_BATCH_SIZE = 32;
/** Default / maximum page sizes for getStreamChunks. */
const DEFAULT_CHUNK_PAGE_SIZE = 32;
const MAX_CHUNK_PAGE_SIZE = 32;

function toBytes(chunk: string | Uint8Array): Uint8Array {
  return typeof chunk === 'string' ? new TextEncoder().encode(chunk) : chunk;
}

export function createStreamer(config: CelldStreamerConfig): CelldStreamer {
  const { env } = config;
  const readPollMs = config.readPollMs ?? READ_POLL_MS;

  const getStreamDO = (streamName: string): StreamDOStub => {
    const id = env.WORKFLOW_STREAMS.idFromName(`stream:${streamName}`);
    return env.WORKFLOW_STREAMS.get(id);
  };

  const getRunRegistryDO = (runId: string): StreamDOStub => {
    const id = env.WORKFLOW_STREAMS.idFromName(`run-streams:${runId}`);
    return env.WORKFLOW_STREAMS.get(id);
  };

  /** Per-isolate cache so each (runId, stream) pair registers only once. */
  const registeredStreams = new Set<string>();

  async function registerStreamForRun(name: string, runId: string): Promise<void> {
    const cacheKey = `${runId}\0${name}`;
    if (registeredStreams.has(cacheKey)) return;
    await getRunRegistryDO(runId).registerStream(runId, name);
    registeredStreams.add(cacheKey);
  }

  const writeToStream = async (
    name: string,
    runId: string | Promise<string>,
    chunk: string | Uint8Array,
  ): Promise<void> => {
    const resolvedRunId = await runId;
    await registerStreamForRun(name, resolvedRunId);
    await getStreamDO(name).writeChunk(resolvedRunId, toBytes(chunk));
  };

  const closeStream = async (name: string, runId: string | Promise<string>): Promise<void> => {
    const resolvedRunId = await runId;
    await registerStreamForRun(name, resolvedRunId);
    await getStreamDO(name).closeStream(resolvedRunId);
  };

  const readFromStream = async (
    name: string,
    startIndex = 0,
  ): Promise<ReadableStream<Uint8Array>> => {
    const stub = getStreamDO(name);

    // Negative startIndex counts back from the current end, clamped to 0.
    let nextIndex: number;
    if (startIndex < 0) {
      const info = await stub.getInfo();
      nextIndex = Math.max(0, info.tailIndex + 1 + startIndex);
    } else {
      nextIndex = startIndex;
    }

    let cancelled = false;

    return new ReadableStream<Uint8Array>({
      async pull(controller) {
        // Loop until we can enqueue data, close, or the reader cancels.
        // Errors from the DO propagate and error the stream — readers must
        // never see a silently-truncated stream.
        while (true) {
          if (cancelled) return;
          const { chunks, done } = await stub.getChunks({
            startIndex: nextIndex,
            limit: READ_BATCH_SIZE,
          });
          if (chunks.length > 0) {
            for (const chunk of chunks) {
              controller.enqueue(chunk);
            }
            nextIndex += chunks.length;
            return;
          }
          if (done) {
            controller.close();
            return;
          }
          await new Promise((resolve) => setTimeout(resolve, readPollMs));
        }
      },

      cancel() {
        cancelled = true;
      },
    });
  };

  const listStreamsByRunId = (runId: string): Promise<string[]> =>
    getRunRegistryDO(runId).listStreams();

  const getStreamChunks = async (
    name: string,
    _runId: string,
    options?: GetChunksOptions,
  ): Promise<StreamChunksResponse> => {
    const limit = Math.min(options?.limit ?? DEFAULT_CHUNK_PAGE_SIZE, MAX_CHUNK_PAGE_SIZE);
    const startIndex = options?.cursor ? Number.parseInt(options.cursor, 10) : 0;
    if (Number.isNaN(startIndex) || startIndex < 0) {
      throw new Error(`Invalid stream cursor: ${options?.cursor}`);
    }

    const stub = getStreamDO(name);
    const result = await stub.getChunks({ startIndex, limit: limit + 1 });
    const hasMore = result.chunks.length > limit;
    const data: StreamChunk[] = result.chunks
      .slice(0, limit)
      .map((chunk, offset) => ({ index: startIndex + offset, data: chunk }));

    return {
      data,
      cursor: hasMore ? String(startIndex + limit) : null,
      hasMore,
      done: result.done,
    };
  };

  const getStreamInfo = (name: string, _runId: string): Promise<StreamInfoResponse> =>
    getStreamDO(name).getInfo();

  return {
    streams: {
      write: (runId, name, chunk) => writeToStream(name, runId, chunk),
      close: (runId, name) => closeStream(name, runId),
      get: (runId, name, startIndex) => {
        void runId;
        return readFromStream(name, startIndex);
      },
      list: listStreamsByRunId,
      getChunks: (runId, name, options) => getStreamChunks(name, runId, options),
      getInfo: (runId, name) => getStreamInfo(name, runId),
    },
    writeToStream,
    closeStream,
    readFromStream,
    listStreamsByRunId,
    getStreamChunks,
    getStreamInfo,
  };
}
