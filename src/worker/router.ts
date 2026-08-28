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
import {
  NATIVE_QUEUE_MAX_MESSAGE_BYTES,
  QUEUE_CLAIM_STALE_MS,
  queueClaimName,
  queueOrphanName,
  queuePayloadObjectKey,
  validateNativeQueueEnvelope,
  validateNativeQueueSendOptions,
  type NativeQueueEnvelope,
  type QueuePayloadOrphan,
  type QueuePayloadRegistration,
} from '../queue-protocol.js';
import { queueDelayDeadline } from '../validation.js';

export const WORLD_NAME = 'world-celld';
export const WORLD_VERSION = '0.1.0';

/** Minimal structural DO namespace type (celld provides the real thing). */
export interface DONamespaceLike {
  idFromName(name: string): { toString(): string };
  get(id: { toString(): string }): unknown;
}

interface NativeQueueBindingLike {
  send(body: string, options?: { contentType?: 'text'; delaySeconds?: number }): Promise<unknown>;
}

interface QueuePayloadBucketLike {
  put(
    key: string,
    value: string,
    options?: { customMetadata?: Record<string, string> },
  ): Promise<unknown>;
  get(key: string): Promise<{ text(): Promise<string> } | null>;
  delete(key: string | string[]): Promise<void>;
}

interface QueueRunStub {
  registerQueuePayload(
    registration: QueuePayloadRegistration,
  ): Promise<{ ok: true } | { ok: false; message: string }>;
  finalizeQueuePayload(messageId: string): Promise<{ ok: true } | { ok: false; message: string }>;
  unregisterQueuePayload(messageId: string): Promise<void>;
  scheduleQueuePayloadOrphan(orphan: QueuePayloadOrphan): Promise<void>;
  cancelQueuePayloadOrphan(messageId: string): Promise<void>;
  reserveQueueMessage(params: {
    messageId: string;
    expiresAt: number;
  }): Promise<{ admitted: boolean; messageId: string }>;
  completeQueueMessage(messageId: string): Promise<void>;
  claimInflight(params: {
    messageId: string;
    staleMs: number;
  }): Promise<{ claimed: boolean; retryAt?: number }>;
  holdInflight(params: {
    messageId: string;
    retryAt: number;
    expiresAt: number;
    reservationExpiresAt?: number;
  }): Promise<{ held: boolean }>;
  releaseInflight(messageId?: string): Promise<void>;
}

export interface WorkerEnv {
  WORKFLOW_DB: DONamespaceLike;
  WORKFLOW_STREAMS: DONamespaceLike;
  WORKFLOW_RUN_CATALOG: DONamespaceLike;
  WORKFLOW_HOOK_TOKENS: DONamespaceLike;
  WORKFLOW_HOOK_IDS: DONamespaceLike;
  WORKFLOW_QUEUE: NativeQueueBindingLike;
  WORKFLOW_QUEUE_PAYLOADS: QueuePayloadBucketLike;
  WORLD_SECRET?: string;
  WORKFLOW_CALLBACK_SECRET?: string;
  WORKFLOW_RETENTION_MS?: string | number;
  WORKFLOW_RETENTION_BATCH_SIZE?: string | number;
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
};

/** Request body cap: oversize payloads get a clear 413 instead of an OOM. */
const MAX_BODY_BYTES = 32 * 1024 * 1024;
const MAX_STREAM_WRITE_BODY_BYTES = MAX_STREAM_BATCH_BYTES + 1024;
const MAX_QUEUE_DELIVERY_RESPONSE_BYTES = 64 * 1024;
const QUEUE_ORPHAN_GRACE_MS = 5 * 24 * 60 * 60 * 1000;
const PERMANENT_QUEUE_STATUSES = new Set([404, 409, 410, 422]);

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

async function readResponseText(response: Response): Promise<string> {
  if (!response.body) return '';
  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let total = 0;
  let text = '';
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      total += value.byteLength;
      if (total > MAX_QUEUE_DELIVERY_RESPONSE_BYTES) {
        await reader.cancel('queue delivery response is too large').catch(() => undefined);
        throw new Error('workflow queue handler response exceeds configured limit');
      }
      text += decoder.decode(value, { stream: true });
    }
    return text + decoder.decode();
  } finally {
    reader.releaseLock();
  }
}

async function parseRpcArguments(request: Request): Promise<unknown[] | Response> {
  if (request.method !== 'POST') {
    return errorResponse(405, 'MethodNotAllowed', 'expected POST');
  }
  if (!hasContentType(request, 'application/json')) {
    return errorResponse(415, 'UnsupportedMediaType', 'expected application/json');
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
  try {
    const text = await readBoundedBody(request);
    if (text === null) {
      return errorResponse(413, 'PayloadTooLarge', `body exceeds ${MAX_BODY_BYTES} bytes`);
    }
    const parsed = rpcParse<unknown>(text);
    return Array.isArray(parsed)
      ? parsed
      : errorResponse(400, 'BadRequest', 'body must be an argument array');
  } catch {
    return errorResponse(400, 'BadRequest', 'malformed rpc body');
  }
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

    if (parts[1] === 'queue' && parts.length === 3) {
      const parsed = await parseRpcArguments(request);
      if (parsed instanceof Response) return parsed;

      if (parts[2] === 'send') {
        if (parsed.length < 1 || parsed.length > 2) {
          return errorResponse(400, 'BadRequest', 'queue send expects an envelope and options');
        }
        if (!env.WORKFLOW_QUEUE) {
          return errorResponse(500, 'WorldMisconfigured', 'missing binding: WORKFLOW_QUEUE');
        }
        let envelope: NativeQueueEnvelope;
        let options;
        try {
          envelope = validateNativeQueueEnvelope(parsed[0]);
          options = validateNativeQueueSendOptions(parsed[1]);
          if (envelope.body === undefined || envelope.payloadKey !== undefined) {
            throw new TypeError('queue send requires an inline body');
          }
        } catch (error) {
          return errorResponse(400, 'BadRequest', (error as Error).message);
        }

        try {
          const namespace = env.WORKFLOW_DB;
          if (envelope.runId && !env.WORKFLOW_QUEUE_PAYLOADS) {
            return errorResponse(
              500,
              'WorldMisconfigured',
              'missing binding: WORKFLOW_QUEUE_PAYLOADS',
            );
          }
          const payloadKey = envelope.runId
            ? queuePayloadObjectKey(envelope.runId, envelope.messageId)
            : undefined;
          const brokerEnvelope: NativeQueueEnvelope = payloadKey
            ? { ...envelope, payloadKey, body: undefined }
            : envelope;
          const encoded = JSON.stringify(brokerEnvelope);
          if (new TextEncoder().encode(encoded).byteLength > NATIVE_QUEUE_MAX_MESSAGE_BYTES) {
            return errorResponse(
              413,
              'PayloadTooLarge',
              `native queue envelope exceeds ${NATIVE_QUEUE_MAX_MESSAGE_BYTES} bytes`,
            );
          }

          const now = Date.now();
          const reservationExpiresAt =
            Math.max(now, envelope.notBefore ?? now) + QUEUE_ORPHAN_GRACE_MS;
          let claim: QueueRunStub | undefined;
          if (envelope.idempotencyKey) {
            claim = namespace.get(
              namespace.idFromName(queueClaimName(envelope.queueName, envelope.idempotencyKey)),
            ) as QueueRunStub;
            const reservation = await claim.reserveQueueMessage({
              messageId: envelope.messageId,
              expiresAt: reservationExpiresAt,
            });
            if (!reservation.admitted) {
              return new Response(rpcStringify({ messageId: reservation.messageId }), {
                status: 200,
                headers: { 'content-type': 'application/json' },
              });
            }
          }

          if (envelope.runId && payloadKey) {
            const run = namespace.get(namespace.idFromName(envelope.runId)) as QueueRunStub;
            const orphanExpiresAt = reservationExpiresAt;
            const registered = await run.registerQueuePayload({
              messageId: envelope.messageId,
              key: payloadKey,
              orphanExpiresAt,
            });
            if (!registered.ok) {
              await claim?.completeQueueMessage(envelope.messageId);
              return errorResponse(410, 'RunExpiredError', registered.message);
            }
            const orphan = namespace.get(
              namespace.idFromName(queueOrphanName(envelope.messageId)),
            ) as QueueRunStub;
            try {
              await orphan.scheduleQueuePayloadOrphan({
                messageId: envelope.messageId,
                runId: envelope.runId,
                key: payloadKey,
                expiresAt: orphanExpiresAt,
              });
            } catch (error) {
              await run.unregisterQueuePayload(envelope.messageId);
              await claim?.completeQueueMessage(envelope.messageId);
              throw error;
            }
            try {
              await env.WORKFLOW_QUEUE_PAYLOADS.put(payloadKey, envelope.body, {
                customMetadata: {
                  runId: envelope.runId,
                  messageId: envelope.messageId,
                },
              });
            } catch (error) {
              await claim?.completeQueueMessage(envelope.messageId);
              throw error;
            }
            let finalized: Awaited<ReturnType<QueueRunStub['finalizeQueuePayload']>>;
            try {
              finalized = await run.finalizeQueuePayload(envelope.messageId);
            } catch (error) {
              await claim?.completeQueueMessage(envelope.messageId);
              throw error;
            }
            if (!finalized.ok) {
              await env.WORKFLOW_QUEUE_PAYLOADS.delete(payloadKey);
              await run.unregisterQueuePayload(envelope.messageId);
              await orphan.cancelQueuePayloadOrphan(envelope.messageId);
              await claim?.completeQueueMessage(envelope.messageId);
              return errorResponse(410, 'RunExpiredError', finalized.message);
            }
          }
          await env.WORKFLOW_QUEUE.send(encoded, {
            contentType: 'text',
            ...options,
          });
          return new Response(rpcStringify({ messageId: envelope.messageId }), {
            status: 200,
            headers: { 'content-type': 'application/json' },
          });
        } catch (error) {
          const err = error as { name?: string; message?: string };
          return errorResponse(500, err.name ?? 'Error', err.message ?? String(error));
        }
      }

      if (parts[2] === 'deliver') {
        if (parsed.length !== 2) {
          return errorResponse(400, 'BadRequest', 'queue deliver expects an envelope and attempt');
        }
        let envelope: NativeQueueEnvelope;
        const attempt = parsed[1];
        try {
          envelope = validateNativeQueueEnvelope(parsed[0]);
          if (!Number.isSafeInteger(attempt) || (attempt as number) < 1) {
            throw new TypeError('queue delivery attempt must be a positive safe integer');
          }
        } catch (error) {
          return errorResponse(400, 'BadRequest', (error as Error).message);
        }

        const namespace = env.WORKFLOW_DB;
        let claim: QueueRunStub | undefined;
        if (envelope.idempotencyKey) {
          claim = namespace.get(
            namespace.idFromName(queueClaimName(envelope.queueName, envelope.idempotencyKey)),
          ) as QueueRunStub;
          const result = await claim.claimInflight({
            messageId: envelope.messageId,
            staleMs: QUEUE_CLAIM_STALE_MS,
          });
          if (!result.claimed) {
            const now = Date.now();
            if (result.retryAt !== undefined && result.retryAt > now) {
              const timeoutSeconds = Math.max(1, Math.ceil((result.retryAt - now) / 1000));
              return Response.json(
                { timeoutSeconds },
                { status: 503, headers: { 'retry-after': String(timeoutSeconds) } },
              );
            }
            return new Response(null, { status: 204 });
          }
        }

        let body = envelope.body;
        if (envelope.payloadKey) {
          if (!env.WORKFLOW_QUEUE_PAYLOADS) {
            await claim?.releaseInflight(envelope.messageId);
            return errorResponse(
              500,
              'WorldMisconfigured',
              'missing binding: WORKFLOW_QUEUE_PAYLOADS',
            );
          }
          body = await (await env.WORKFLOW_QUEUE_PAYLOADS.get(envelope.payloadKey))?.text();
          if (body === undefined) {
            if (envelope.runId) {
              const run = namespace.get(namespace.idFromName(envelope.runId)) as QueueRunStub;
              const orphan = namespace.get(
                namespace.idFromName(queueOrphanName(envelope.messageId)),
              ) as QueueRunStub;
              await run.unregisterQueuePayload(envelope.messageId);
              await orphan.cancelQueuePayloadOrphan(envelope.messageId);
            }
            await claim?.completeQueueMessage(envelope.messageId);
            return errorResponse(410, 'QueuePayloadExpired', 'workflow queue payload is gone');
          }
        }

        let callback: Response;
        try {
          const headers: Record<string, string> = {
            'content-type': 'application/json',
            'x-vqs-queue-name': envelope.queueName,
            'x-vqs-message-id': envelope.messageId,
            'x-vqs-message-attempt': String(attempt),
          };
          if (env.WORKFLOW_CALLBACK_SECRET) {
            headers['x-workflow-callback-secret'] = env.WORKFLOW_CALLBACK_SECRET;
          }
          callback = await fetch(
            `${envelope.targetBaseUrl.replace(/\/$/, '')}/.well-known/workflow/v1/flow`,
            {
              method: 'POST',
              headers,
              body,
              signal: AbortSignal.timeout(300_000),
            },
          );
        } catch (error) {
          await claim?.releaseInflight(envelope.messageId);
          return errorResponse(502, 'QueueDeliveryError', String(error));
        }

        const finished = callback.ok || PERMANENT_QUEUE_STATUSES.has(callback.status);
        let responseBody = '';
        if (finished) {
          await callback.body?.cancel().catch(() => undefined);
        } else {
          try {
            responseBody = await readResponseText(callback);
          } catch (error) {
            await claim?.releaseInflight(envelope.messageId);
            return errorResponse(502, 'QueueDeliveryError', (error as Error).message);
          }
        }
        if (finished) {
          try {
            if (envelope.payloadKey) {
              await env.WORKFLOW_QUEUE_PAYLOADS.delete(envelope.payloadKey);
              if (envelope.runId) {
                const run = namespace.get(namespace.idFromName(envelope.runId)) as QueueRunStub;
                const orphan = namespace.get(
                  namespace.idFromName(queueOrphanName(envelope.messageId)),
                ) as QueueRunStub;
                await run.unregisterQueuePayload(envelope.messageId);
                await orphan.cancelQueuePayloadOrphan(envelope.messageId);
              }
            }
            await claim?.completeQueueMessage(envelope.messageId);
          } catch (error) {
            await claim?.releaseInflight(envelope.messageId);
            return errorResponse(500, 'QueuePayloadCleanupError', String(error));
          }
        } else if (callback.status === 503) {
          try {
            const timeoutSeconds = (JSON.parse(responseBody) as { timeoutSeconds?: unknown })
              .timeoutSeconds;
            const notBefore = queueDelayDeadline(
              Date.now(),
              timeoutSeconds,
              1,
              QUEUE_CLAIM_STALE_MS,
            );
            if (notBefore === null) {
              throw new Error('workflow queue suspension deadline is invalid');
            }
            if (envelope.payloadKey && envelope.runId) {
              const orphan = namespace.get(
                namespace.idFromName(queueOrphanName(envelope.messageId)),
              ) as QueueRunStub;
              await orphan.scheduleQueuePayloadOrphan({
                messageId: envelope.messageId,
                runId: envelope.runId,
                key: envelope.payloadKey,
                expiresAt: notBefore + QUEUE_ORPHAN_GRACE_MS,
              });
            }
            if (claim) {
              await claim.holdInflight({
                messageId: envelope.messageId,
                retryAt: notBefore,
                expiresAt: notBefore + QUEUE_CLAIM_STALE_MS,
                reservationExpiresAt: notBefore + QUEUE_ORPHAN_GRACE_MS,
              });
            }
          } catch (error) {
            await claim?.releaseInflight(envelope.messageId);
            return errorResponse(500, 'QueueSuspensionError', String(error));
          }
        } else if (claim) {
          await claim.releaseInflight(envelope.messageId);
        }

        const responseHasNoBody =
          callback.status === 204 || callback.status === 205 || callback.status === 304;
        return new Response(responseHasNoBody ? null : responseBody, {
          status: callback.status,
          headers: {
            'content-type': callback.headers.get('content-type') ?? 'application/json',
            ...(callback.headers.get('retry-after')
              ? { 'retry-after': callback.headers.get('retry-after')! }
              : {}),
          },
        });
      }

      return errorResponse(404, 'NotFound', `unknown queue operation: ${parts[2]}`);
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
