/**
 * Worker-level HTTP router: the fleet-facing RPC surface of the celld world.
 *
 * Routes:
 *   GET  /v1/health                              — liveness + spec version (unauthenticated)
 *   POST /v1/rpc/{binding}/{name}/{method}       — DO RPC (bearer auth)
 *   POST /v1/index/{domain}/{method}             — cohesive index operation
 *   GET  /v1/streams/{name}/chunks               — bounded binary long-poll read
 *   POST /v1/streams/{name}/chunks               — bounded binary batch append
 *
 * Generic RPC bodies use the tagged JSON codec. Stream chunk bodies use the
 * compact binary stream protocol. Only whitelisted routes/methods dispatch.
 */
import { SPEC_VERSION_CURRENT, type WorkflowRun } from '@workflow/world';
import { rpcParse, rpcStringify } from '../codec.js';
import type { HookTokenOwner } from '../config.js';
import {
  createWorkflowIndex,
  type CellNamespaceLike,
  type HookReservation,
  type HookIdShardStub,
  type HookTokenShardStub,
  type IndexListOptions,
  type RunCatalogShardStub,
} from '../indexes.js';
import type { ExpireRunIndexesRequest, ReleaseHookIndexesRequest } from '../retention.js';
import {
  MAX_STREAM_BATCH_BYTES,
  MAX_STREAM_CHUNK_BYTES,
  MAX_STREAM_LONG_POLL_MS,
  MAX_STREAM_READ_BYTES,
  MAX_STREAM_READ_CHUNKS,
  STREAM_BATCH_CONTENT_TYPE,
  decodeStreamWriteBatch,
  encodeStreamReadResult,
  encodeStreamWriteResult,
  type StreamReadRequest,
  type StreamReadResult,
  type StreamWriteResult,
} from '../stream-protocol.js';
import { authenticate } from './auth.js';

export const WORLD_NAME = 'world-celld';
export const WORLD_VERSION = '0.1.0';

/** Minimal structural DO namespace type (celld provides the real thing). */
export interface DONamespaceLike {
  idFromName(name: string): { toString(): string };
  get(id: { toString(): string }): unknown;
}

export interface WorkerEnv {
  WORKFLOW_DB: DONamespaceLike;
  WORKFLOW_STREAMS: DONamespaceLike;
  WORKFLOW_RUN_CATALOG: DONamespaceLike;
  WORKFLOW_HOOK_TOKENS: DONamespaceLike;
  WORKFLOW_HOOK_IDS: DONamespaceLike;
  WORKFLOW_QUEUE: DONamespaceLike;
  WORLD_SECRET?: string;
}

function workflowIndex(env: WorkerEnv) {
  return createWorkflowIndex({
    runCatalog: env.WORKFLOW_RUN_CATALOG as CellNamespaceLike<RunCatalogShardStub>,
    hookTokens: env.WORKFLOW_HOOK_TOKENS as CellNamespaceLike<HookTokenShardStub>,
    hookIds: env.WORKFLOW_HOOK_IDS as CellNamespaceLike<HookIdShardStub>,
  });
}

const BINDINGS: Record<string, { env: keyof WorkerEnv; methods: ReadonlySet<string> }> = {
  runs: {
    env: 'WORKFLOW_DB',
    methods: new Set([
      'applyEvent',
      'getLifecycleStatus',
      'getRun',
      'getStep',
      'getEvent',
      'listEvents',
      'listSteps',
      'listHooks',
      'getCleanupStatus',
      'scheduleCleanup',
      'cleanupNow',
      'rearmCleanup',
      'resolveHookTokenClaim',
    ]),
  },
  streams: {
    env: 'WORKFLOW_STREAMS',
    methods: new Set([
      'closeStream',
      'failStream',
      'registerStream',
      'listStreams',
      'expireRegistry',
      'finalizeRegistry',
      'expireStream',
    ]),
  },
  'hook-tokens': {
    env: 'WORKFLOW_HOOK_TOKENS',
    methods: new Set(['get']),
  },
  'hook-ids': {
    env: 'WORKFLOW_HOOK_IDS',
    methods: new Set(['get']),
  },
  queue: {
    env: 'WORKFLOW_QUEUE',
    methods: new Set([
      'enqueue',
      'stats',
      'listDeadLetters',
      'redriveDeadLetter',
      'purgeDeadLetters',
      'rearmAlarm',
      'expireRun',
      'acknowledgeExpireRun',
    ]),
  },
};

/** Request body cap: oversize payloads get a clear 413 instead of an OOM. */
const MAX_BODY_BYTES = 32 * 1024 * 1024;
const MAX_STREAM_WRITE_BODY_BYTES = MAX_STREAM_BATCH_BYTES + 1024;

function errorResponse(status: number, name: string, message: string): Response {
  return Response.json({ error: { name, message } }, { status });
}

async function readBoundedBody(request: Request): Promise<string | null> {
  if (!request.body) return '';
  const reader = request.body.getReader();
  const decoder = new TextDecoder();
  let total = 0;
  let text = '';
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      total += value.byteLength;
      if (total > MAX_BODY_BYTES) {
        await reader.cancel('RPC body exceeds configured limit').catch(() => undefined);
        return null;
      }
      text += decoder.decode(value, { stream: true });
    }
    return text + decoder.decode();
  } finally {
    reader.releaseLock();
  }
}

async function readBoundedBytes(request: Request, maxBytes: number): Promise<Uint8Array | null> {
  if (!request.body) return new Uint8Array();
  const reader = request.body.getReader();
  const chunks: Uint8Array[] = [];
  let total = 0;
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      total += value.byteLength;
      if (total > maxBytes) {
        await reader.cancel('stream body exceeds configured limit').catch(() => undefined);
        return null;
      }
      chunks.push(value);
    }
  } finally {
    reader.releaseLock();
  }
  const body = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    body.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return body;
}

function parseBoundedInteger(url: URL, name: string, minimum: number, maximum: number): number {
  const raw = url.searchParams.get(name);
  const value = raw === null ? Number.NaN : Number(raw);
  if (!Number.isSafeInteger(value) || value < minimum || value > maximum) {
    const error = new Error(`${name} must be an integer between ${minimum} and ${maximum}`);
    error.name = 'StreamProtocolError';
    throw error;
  }
  return value;
}

function streamResponse(body: Uint8Array): Response {
  return new Response(body, {
    status: 200,
    headers: { 'content-type': STREAM_BATCH_CONTENT_TYPE },
  });
}

export function createRouter(env: WorkerEnv) {
  return async (request: Request): Promise<Response> => {
    const url = new URL(request.url);
    const parts = url.pathname.split('/').filter(Boolean);

    if (parts[0] !== 'v1') {
      return errorResponse(404, 'NotFound', `unknown path: ${url.pathname}`);
    }

    if (parts[1] === 'health' && parts.length === 2) {
      return Response.json({
        ok: true,
        name: WORLD_NAME,
        version: WORLD_VERSION,
        specVersion: SPEC_VERSION_CURRENT,
      });
    }

    const auth = await authenticate(request, env.WORLD_SECRET);
    if (!auth.ok) {
      return auth.response;
    }

    if (parts[1] === 'index' && parts.length === 4) {
      if (request.method !== 'POST') {
        return errorResponse(405, 'MethodNotAllowed', 'expected POST');
      }
      const contentLength = Number(request.headers.get('content-length') ?? '0');
      if (contentLength > MAX_BODY_BYTES) {
        return errorResponse(413, 'PayloadTooLarge', `body exceeds ${MAX_BODY_BYTES} bytes`);
      }
      let args: unknown[];
      try {
        const text = await readBoundedBody(request);
        if (text === null) {
          return errorResponse(413, 'PayloadTooLarge', `body exceeds ${MAX_BODY_BYTES} bytes`);
        }
        const parsed = rpcParse<unknown>(text);
        if (!Array.isArray(parsed)) {
          return errorResponse(400, 'BadRequest', 'body must be an argument array');
        }
        args = parsed;
      } catch {
        return errorResponse(400, 'BadRequest', 'malformed rpc body');
      }
      try {
        const index = workflowIndex(env);
        let result: unknown;
        const operation = `${parts[2]}.${parts[3]}`;
        if (operation === 'runs.list') {
          result = await index.listRuns(args[0] as IndexListOptions | undefined);
        } else if (operation === 'runs.commit') {
          result = await index.commitRun(
            args[0] as WorkflowRun,
            args[1] as string,
            args[2] as number,
          );
        } else if (operation === 'runs.expire') {
          result = await index.expireRun(args[0] as ExpireRunIndexesRequest);
        } else if (operation === 'hooks.reserve') {
          result = await index.reserveHook(args[0] as string, args[1] as HookTokenOwner);
        } else if (operation === 'hooks.finalize') {
          result = await index.finalizeHookIndexes(
            args[0] as string,
            args[1] as string,
            args[2] as string,
            args[3] as HookTokenOwner,
            args[4] as HookReservation | undefined,
          );
        } else if (operation === 'hooks.release-reservation') {
          result = await index.releaseHookReservation(
            args[0] as string,
            args[1] as HookTokenOwner,
            args[2] as HookReservation,
          );
        } else if (operation === 'hooks.release') {
          result = await index.releaseHookIndexes(args[0] as ReleaseHookIndexesRequest);
        } else {
          return errorResponse(404, 'NotFound', `unknown index operation: ${operation}`);
        }
        return new Response(rpcStringify(result ?? null), {
          status: 200,
          headers: { 'content-type': 'application/json' },
        });
      } catch (error) {
        const err = error as { name?: string; message?: string; status?: number };
        return Response.json(
          {
            error: {
              name: err?.name ?? 'Error',
              message: err?.message ?? String(error),
              status: typeof err?.status === 'number' ? err.status : undefined,
            },
          },
          { status: 500 },
        );
      }
    }

    if (parts[1] === 'streams' && parts.length === 4 && parts[3] === 'chunks') {
      const namespace = env.WORKFLOW_STREAMS;
      if (!namespace) {
        return errorResponse(500, 'WorldMisconfigured', 'missing binding: WORKFLOW_STREAMS');
      }
      const name = decodeURIComponent(parts[2]);
      const runId = url.searchParams.get('runId');
      if (!runId) return errorResponse(400, 'BadRequest', 'runId is required');
      const stub = namespace.get(namespace.idFromName(name)) as {
        writeChunks(runId: string, chunks: Uint8Array[]): Promise<StreamWriteResult>;
        readChunks(request: StreamReadRequest, signal?: AbortSignal): Promise<StreamReadResult>;
      };

      try {
        if (request.method === 'POST') {
          if (!request.headers.get('content-type')?.startsWith(STREAM_BATCH_CONTENT_TYPE)) {
            return errorResponse(415, 'UnsupportedMediaType', STREAM_BATCH_CONTENT_TYPE);
          }
          const contentLength = Number(request.headers.get('content-length') ?? '0');
          if (contentLength > MAX_STREAM_WRITE_BODY_BYTES) {
            return errorResponse(
              413,
              'PayloadTooLarge',
              `body exceeds ${MAX_STREAM_WRITE_BODY_BYTES} bytes`,
            );
          }
          const body = await readBoundedBytes(request, MAX_STREAM_WRITE_BODY_BYTES);
          if (body === null) {
            return errorResponse(
              413,
              'PayloadTooLarge',
              `body exceeds ${MAX_STREAM_WRITE_BODY_BYTES} bytes`,
            );
          }
          const result = await stub.writeChunks(runId, decodeStreamWriteBatch(body));
          return streamResponse(encodeStreamWriteResult(result));
        }

        if (request.method === 'GET') {
          const result = await stub.readChunks(
            {
              runId,
              startIndex: parseBoundedInteger(url, 'startIndex', 0, 0x7fffffff),
              maxChunks: parseBoundedInteger(url, 'maxChunks', 0, MAX_STREAM_READ_CHUNKS),
              maxBytes: parseBoundedInteger(
                url,
                'maxBytes',
                MAX_STREAM_CHUNK_BYTES,
                MAX_STREAM_READ_BYTES,
              ),
              waitMs: parseBoundedInteger(url, 'waitMs', 0, MAX_STREAM_LONG_POLL_MS),
            },
            request.signal,
          );
          return streamResponse(encodeStreamReadResult(result));
        }

        return errorResponse(405, 'MethodNotAllowed', 'expected GET or POST');
      } catch (error) {
        const err = error as { name?: string; message?: string; status?: number };
        return Response.json(
          {
            error: {
              name: err?.name ?? 'Error',
              message: err?.message ?? String(error),
              status: typeof err?.status === 'number' ? err.status : undefined,
            },
          },
          { status: err?.name === 'StreamProtocolError' ? 400 : 500 },
        );
      }
    }

    if (parts[1] !== 'rpc') {
      return errorResponse(404, 'NotFound', `unknown path: ${url.pathname}`);
    }

    if (request.method !== 'POST' || parts.length !== 5) {
      return errorResponse(404, 'NotFound', 'expected POST /v1/rpc/{binding}/{name}/{method}');
    }

    const [, , bindingKey, encodedName, method] = parts;
    const binding = BINDINGS[bindingKey];
    if (!binding) {
      return errorResponse(404, 'NotFound', `unknown binding: ${bindingKey}`);
    }
    if (!binding.methods.has(method)) {
      return errorResponse(404, 'NotFound', `unknown method: ${bindingKey}.${method}`);
    }

    const contentLength = Number(request.headers.get('content-length') ?? '0');
    if (contentLength > MAX_BODY_BYTES) {
      return errorResponse(413, 'PayloadTooLarge', `body exceeds ${MAX_BODY_BYTES} bytes`);
    }

    let args: unknown[];
    try {
      const text = await readBoundedBody(request);
      if (text === null) {
        return errorResponse(413, 'PayloadTooLarge', `body exceeds ${MAX_BODY_BYTES} bytes`);
      }
      const parsed = rpcParse<unknown>(text);
      if (!Array.isArray(parsed)) {
        return errorResponse(400, 'BadRequest', 'body must be an argument array');
      }
      args = parsed;
    } catch {
      return errorResponse(400, 'BadRequest', 'malformed rpc body');
    }

    const name = decodeURIComponent(encodedName);
    const namespace = env[binding.env] as DONamespaceLike | undefined;
    if (!namespace) {
      return errorResponse(500, 'WorldMisconfigured', `missing binding: ${binding.env}`);
    }

    try {
      const stub = namespace.get(namespace.idFromName(name)) as Record<
        string,
        (...a: unknown[]) => Promise<unknown>
      >;
      const result = await stub[method](...args);
      return new Response(rpcStringify(result ?? null), {
        status: 200,
        headers: { 'content-type': 'application/json' },
      });
    } catch (error) {
      // Guard failures travel as structured outcomes, so a thrown error here
      // is exceptional (e.g. writing to a closed stream). Serialize by name
      // for client-side reconstruction.
      const err = error as { name?: string; message?: string; status?: number };
      return Response.json(
        {
          error: {
            name: err?.name ?? 'Error',
            message: err?.message ?? String(error),
            status: typeof err?.status === 'number' ? err.status : undefined,
          },
        },
        { status: 500 },
      );
    }
  };
}
