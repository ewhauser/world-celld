/**
 * Worker-level HTTP router: the fleet-facing RPC surface of the celld world.
 *
 * Routes:
 *   GET  /v1/health                              — liveness + spec version (unauthenticated)
 *   POST /v1/rpc/{binding}/{name}/{method}       — DO RPC (bearer auth)
 *
 * The RPC body is an rpc-codec-encoded JSON array of arguments; the response
 * body is the rpc-codec-encoded return value. Only whitelisted methods
 * dispatch — everything else (including fetch/alarm) is unreachable.
 */
import { SPEC_VERSION_CURRENT } from '@workflow/world';
import { rpcParse, rpcStringify } from '../codec.js';
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
  WORKFLOW_INDEX: DONamespaceLike;
  WORKFLOW_QUEUE: DONamespaceLike;
  WORLD_SECRET?: string;
}

const BINDINGS: Record<string, { env: keyof WorkerEnv; methods: ReadonlySet<string> }> = {
  runs: {
    env: 'WORKFLOW_DB',
    methods: new Set([
      'applyEvent',
      'getRun',
      'getStep',
      'getEvent',
      'listEvents',
      'listSteps',
      'listHooks',
    ]),
  },
  streams: {
    env: 'WORKFLOW_STREAMS',
    methods: new Set([
      'writeChunk',
      'closeStream',
      'getChunks',
      'getInfo',
      'registerStream',
      'listStreams',
    ]),
  },
  index: {
    env: 'WORKFLOW_INDEX',
    methods: new Set([
      'get',
      'put',
      'delete',
      'list',
      'reserveHookToken',
      'finalizeHookIndexes',
      'releaseHookToken',
      'deleteHookIndexes',
    ]),
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
    ]),
  },
};

/** Request body cap: oversize payloads get a clear 413 instead of an OOM. */
const MAX_BODY_BYTES = 32 * 1024 * 1024;

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

    if (parts[1] !== 'rpc') {
      return errorResponse(404, 'NotFound', `unknown path: ${url.pathname}`);
    }

    const auth = await authenticate(request, env.WORLD_SECRET);
    if (!auth.ok) {
      return auth.response;
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
