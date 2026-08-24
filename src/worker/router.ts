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
import { SPEC_VERSION_CURRENT } from '@workflow/world';
import { parseApplyEventRequest } from '../apply-event.js';
import { rpcParse, rpcStringify } from '../codec.js';
import {
  createWorkflowIndex,
  type CellNamespaceLike,
  type HookIdShardStub,
  type HookTokenShardStub,
  type RunCatalogShardStub,
} from '../indexes.js';
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
import { INDEX_OPERATIONS, type IndexOperation, validateIndexRequest } from './index-validation.js';

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
  WORKFLOW_RETENTION_MS?: string | number;
  WORKFLOW_RETENTION_BATCH_SIZE?: string | number;
  WORKFLOW_RETENTION_QUEUE_SHARDS?: string | number;
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

function hasContentType(request: Request, expected: string): boolean {
  return (
    request.headers.get('content-type')?.split(';', 1)[0].trim().toLowerCase() ===
    expected.toLowerCase()
  );
}

function declaredContentLength(request: Request): number | null {
  const raw = request.headers.get('content-length');
  if (raw === null) return null;
  if (!/^\d+$/.test(raw)) {
    const error = new Error('content-length must be a non-negative integer');
    error.name = 'RequestValidationError';
    throw error;
  }
  const value = Number(raw);
  if (!Number.isSafeInteger(value)) {
    const error = new Error('content-length is out of range');
    error.name = 'RequestValidationError';
    throw error;
  }
  return value;
}

function decodePathComponent(value: string, name: string): string {
  try {
    return decodeURIComponent(value);
  } catch {
    const error = new Error(`${name} has malformed percent encoding`);
    error.name = 'RequestValidationError';
    throw error;
  }
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
  const value = raw !== null && /^\d+$/.test(raw) ? Number(raw) : Number.NaN;
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
      if (request.method !== 'GET') {
        return errorResponse(405, 'MethodNotAllowed', 'expected GET');
      }
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
      if (!hasContentType(request, 'application/json')) {
        return errorResponse(415, 'UnsupportedMediaType', 'expected application/json');
      }
      for (const required of [
        'WORKFLOW_RUN_CATALOG',
        'WORKFLOW_HOOK_TOKENS',
        'WORKFLOW_HOOK_IDS',
      ] as const) {
        if (!env[required]) {
          return errorResponse(500, 'WorldMisconfigured', `missing binding: ${required}`);
        }
      }
      let contentLength: number | null;
      try {
        contentLength = declaredContentLength(request);
      } catch (error) {
        return errorResponse(400, 'BadRequest', (error as Error).message);
      }
      if (contentLength !== null && contentLength > MAX_BODY_BYTES) {
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
      const operation = `${parts[2]}.${parts[3]}`;
      if (!INDEX_OPERATIONS.has(operation as IndexOperation)) {
        return errorResponse(404, 'NotFound', `unknown index operation: ${operation}`);
      }
      let validated;
      try {
        validated = validateIndexRequest(operation as IndexOperation, args);
      } catch (error) {
        return errorResponse(400, 'BadRequest', (error as Error).message);
      }
      try {
        const index = workflowIndex(env);
        let result: unknown;
        switch (validated.operation) {
          case 'runs.list':
            result = await index.listRuns(...validated.args);
            break;
          case 'runs.commit':
            result = await index.commitRun(...validated.args);
            break;
          case 'runs.expire':
            result = await index.expireRun(...validated.args);
            break;
          case 'hooks.reserve':
            result = await index.reserveHook(...validated.args);
            break;
          case 'hooks.finalize':
            result = await index.finalizeHookIndexes(...validated.args);
            break;
          case 'hooks.release-reservation':
            result = await index.releaseHookReservation(...validated.args);
            break;
          case 'hooks.release':
            result = await index.releaseHookIndexes(...validated.args);
            break;
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
      const runId = url.searchParams.get('runId');
      if (!runId) return errorResponse(400, 'BadRequest', 'runId is required');
      let name: string;
      try {
        name = decodePathComponent(parts[2], 'stream name');
      } catch (error) {
        return errorResponse(400, 'BadRequest', (error as Error).message);
      }

      try {
        if (request.method === 'POST') {
          if (!hasContentType(request, STREAM_BATCH_CONTENT_TYPE)) {
            return errorResponse(415, 'UnsupportedMediaType', STREAM_BATCH_CONTENT_TYPE);
          }
          const contentLength = declaredContentLength(request);
          if (contentLength !== null && contentLength > MAX_STREAM_WRITE_BODY_BYTES) {
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
          const chunks = decodeStreamWriteBatch(body);
          const namespace = env.WORKFLOW_STREAMS;
          if (!namespace) {
            return errorResponse(500, 'WorldMisconfigured', 'missing binding: WORKFLOW_STREAMS');
          }
          const stub = namespace.get(namespace.idFromName(name)) as {
            writeChunks(runId: string, chunks: Uint8Array[]): Promise<StreamWriteResult>;
          };
          const result = await stub.writeChunks(runId, chunks);
          return streamResponse(encodeStreamWriteResult(result));
        }

        if (request.method === 'GET') {
          const readRequest = {
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
          } satisfies StreamReadRequest;
          const namespace = env.WORKFLOW_STREAMS;
          if (!namespace) {
            return errorResponse(500, 'WorldMisconfigured', 'missing binding: WORKFLOW_STREAMS');
          }
          const stub = namespace.get(namespace.idFromName(name)) as {
            readChunks(request: StreamReadRequest, signal?: AbortSignal): Promise<StreamReadResult>;
          };
          const result = await stub.readChunks(readRequest, request.signal);
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
          {
            status:
              err?.name === 'StreamProtocolError' || err?.name === 'RequestValidationError'
                ? 400
                : 500,
          },
        );
      }
    }

    if (parts[1] !== 'rpc') {
      return errorResponse(404, 'NotFound', `unknown path: ${url.pathname}`);
    }

    if (parts.length !== 5) {
      return errorResponse(404, 'NotFound', 'expected POST /v1/rpc/{binding}/{name}/{method}');
    }
    if (request.method !== 'POST') {
      return errorResponse(405, 'MethodNotAllowed', 'expected POST');
    }

    const [, , bindingKey, encodedName, method] = parts;
    const binding = BINDINGS[bindingKey];
    if (!binding) {
      return errorResponse(404, 'NotFound', `unknown binding: ${bindingKey}`);
    }
    if (!binding.methods.has(method)) {
      return errorResponse(404, 'NotFound', `unknown method: ${bindingKey}.${method}`);
    }

    if (!hasContentType(request, 'application/json')) {
      return errorResponse(415, 'UnsupportedMediaType', 'expected application/json');
    }
    let name: string;
    try {
      name = decodePathComponent(encodedName, 'Durable Object name');
    } catch (error) {
      return errorResponse(400, 'BadRequest', (error as Error).message);
    }
    const namespace = env[binding.env] as DONamespaceLike | undefined;
    if (!namespace) {
      return errorResponse(500, 'WorldMisconfigured', `missing binding: ${binding.env}`);
    }
    let contentLength: number | null;
    try {
      contentLength = declaredContentLength(request);
    } catch (error) {
      return errorResponse(400, 'BadRequest', (error as Error).message);
    }
    if (contentLength !== null && contentLength > MAX_BODY_BYTES) {
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

    if (bindingKey === 'runs' && method === 'applyEvent') {
      if (args.length !== 1) {
        return errorResponse(400, 'BadRequest', 'applyEvent expects exactly one request argument');
      }
      try {
        const applyRequest = parseApplyEventRequest(args[0]);
        if (applyRequest.runId !== name) {
          return errorResponse(
            400,
            'BadRequest',
            `applyEvent runId "${applyRequest.runId}" does not match routed run "${name}"`,
          );
        }
        args = [applyRequest];
      } catch (error) {
        return errorResponse(400, 'BadRequest', (error as Error).message);
      }
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
