/**
 * Queue implementation for the celld world.
 *
 * Vendored from vinnymac/worlds packages/world-cloudflare/src/queue.ts
 * (Apache-2.0, see NOTICE), modified for celld:
 *
 * - The Cloudflare Queues producer is replaced by an `enqueue` RPC to a
 *   QueueDO cell (`q:<shard>`), which owns the message lifecycle: scheduling
 *   via durable alarms, delivery via outbound fetch, retries with capped
 *   backoff, and a dead-letter table.
 * - The consumer-side claim-DO idempotency mechanism is gone: a queue cell is
 *   a single serialized writer, so idempotencyKey dedup happens inside the
 *   cell (at enqueue and across the delivery state machine).
 * - `createQueueHandler` has ONE dialect — the x-vqs wire format the test
 *   pump has always used — extended so permanent errors surface as their
 *   HTTP statuses. QueueDO deliveries and the in-process test pump hit the
 *   exact same handler path.
 */
import { setTimeout as delay } from 'node:timers/promises';
import { WorkflowWorldError } from '@workflow/errors';
import { RunExpiredError } from '@workflow/errors';
import {
  MessageId,
  parseQueueName,
  type Queue,
  type QueuePayload,
  type ValidQueueName,
} from '@workflow/world';
import { parse, stringify } from './vendor/shared/index.js';
import { monotonicFactory } from 'ulid';
import { debug } from './util.js';

/**
 * HTTP status codes that indicate permanent (non-retryable) failures.
 * Deliveries with these statuses are dropped immediately instead of retried.
 *
 * - 404: Resource not found (e.g., run was deleted)
 * - 409: Conflict (e.g., duplicate event that can't be replayed)
 * - 410: Gone (e.g., run was already terminal)
 * - 422: Unprocessable entity (e.g., invalid payload structure)
 */
export const PERMANENT_ERROR_STATUSES = new Set([404, 409, 410, 422]);

function isPermanentError(err: unknown): err is WorkflowWorldError {
  if (err instanceof WorkflowWorldError && err.status !== undefined) {
    return PERMANENT_ERROR_STATUSES.has(err.status);
  }
  return false;
}

/** Caps at 60 seconds. */
export function computeBackoff(attempt: number): number {
  return Math.min(60, 2 ** attempt);
}

type Pathname = 'flow';

const QUEUE_PATHNAME: Pathname = 'flow';

function isTestMode(): boolean {
  // Explicit override: force the production QueueDO path even under a test
  // runner (used by the conformance suite to exercise live queue cells —
  // the world-testing server inherits VITEST from the vitest parent).
  if (process.env.CELLD_QUEUE_MODE === 'cells') return false;
  return process.env.VITEST === 'true' || process.env.NODE_ENV === 'test';
}

/**
 * Enqueue request accepted by a QueueDO cell. The message payload travels
 * pre-encoded with the shared tagged-JSON codec (`body`), so the cell can
 * forward it byte-identical to the app's queue handler without re-encoding.
 */
export interface EnqueueRequest {
  messageId: string;
  queueName: ValidQueueName;
  pathname: Pathname;
  /** `stringify(message)` — the exact bytes POSTed to the app on delivery. */
  body: string;
  /** Owning workflow run; absent only for health-check messages. */
  runId?: string;
  idempotencyKey?: string;
  delaySeconds?: number;
  /**
   * Delivery configuration. The cell pins this from its first enqueue (the
   * eve-ambient pattern); a later mismatch is rejected as CONFIG_MISMATCH.
   */
  config: QueueCellConfig;
}

export interface QueueCellConfig {
  /** Base URL of the app; deliveries POST to `${baseUrl}/.well-known/workflow/v1/flow`. */
  targetBaseUrl: string;
  queueShards: number;
}

export type EnqueueOutcome =
  | { ok: true; messageId: string; deduped: boolean }
  | { ok: false; code: 'CONFIG_MISMATCH' | 'RUN_EXPIRED'; message: string };

/** RPC surface of QueueDO used by the queue producer. */
export interface QueueCellStub {
  enqueue(request: EnqueueRequest): Promise<EnqueueOutcome>;
  expireRun(
    runId: string,
    expiredAt: number,
    options?: { limit?: number },
  ): Promise<import('./retention.js').ExpireQueueRunResult>;
  acknowledgeExpireRun(
    runId: string,
    receipt: import('./retention.js').QueueExpiryReceipt,
  ): Promise<import('./retention.js').AcknowledgeQueueExpiryResult>;
}

export interface QueueCellNamespace {
  idFromName(name: string): { toString(): string };
  get(id: { toString(): string }): QueueCellStub;
}

export interface CelldQueueConfig {
  env: {
    WORKFLOW_QUEUE: QueueCellNamespace;
  };
  deploymentId: string;
  /**
   * Base URL the app's workflow endpoints are mounted on. QueueDO cells
   * deliver to `${baseUrl}/.well-known/workflow/v1/flow`; the test
   * pump uses the same value.
   * Default: process.env.WORKFLOW_BASE_URL || `http://localhost:${process.env.PORT ?? 3000}`
   */
  baseUrl?: string;
  /** Number of `q:<shard>` cells to spread enqueues over. Default: 1 */
  queueShards?: number;
  /** Per-job HTTP request timeout (ms) for the test pump. Default: 300_000 */
  httpTimeoutMs?: number;
  /** Maximum retry attempts in the test pump before dropping a job. Default: 5 */
  maxAttempts?: number;
  /** Base backoff delay (ms) for test pump retries. Default: 1000 */
  backoffDelayMs?: number;
}

interface PumpEnvelope {
  messageId: string;
  queueName: ValidQueueName;
  attempt: number;
  message: QueuePayload;
  idempotencyKey?: string;
}

function resolveBaseUrl(config: CelldQueueConfig): string {
  if (config.baseUrl) return config.baseUrl;
  if (process.env.WORKFLOW_BASE_URL) return process.env.WORKFLOW_BASE_URL;
  const port = process.env.PORT ?? '3000';
  return `http://localhost:${port}`;
}

/**
 * FNV-1a hash for shard selection. All messages sharing an idempotencyKey
 * land on the same cell so dedup stays strictly consistent.
 */
export function shardFor(key: string, shards: number): number {
  if (shards <= 1) return 0;
  let hash = 0x811c9dc5;
  for (let i = 0; i < key.length; i++) {
    hash ^= key.charCodeAt(i);
    hash = Math.imul(hash, 0x01000193);
  }
  return (hash >>> 0) % shards;
}

/**
 * In-process test pump. Holds an in-memory FIFO and HTTP-dispatches envelopes
 * to the user's server at `${baseUrl}/.well-known/workflow/v1/flow`.
 *
 * Mirrors world-local's idempotency semantics: messages are deduplicated on
 * `idempotencyKey` while a message with the same key is in flight, and the
 * key is released only when the message is fully handled (success or drop).
 *
 * Modified vs upstream: permanent-error statuses (404/409/410/422) from the
 * handler drop the message immediately instead of burning retry budget —
 * matching QueueDO's production semantics.
 */
function createTestPump(config: CelldQueueConfig) {
  const httpTimeoutMs = config.httpTimeoutMs ?? 300_000;
  const maxAttempts = config.maxAttempts ?? 5;
  const baseBackoffMs = config.backoffDelayMs ?? 1000;

  const queues: Record<Pathname, PumpEnvelope[]> = { flow: [] };
  const wakers: Record<Pathname, Array<() => void>> = { flow: [] };
  /** Inflight messageIds by idempotencyKey (world-local queue.js semantics). */
  const inflightMessages = new Map<string, MessageId>();
  let running = false;

  function release(envelope: PumpEnvelope) {
    if (envelope.idempotencyKey) {
      inflightMessages.delete(envelope.idempotencyKey);
    }
  }

  function enqueue(pathname: Pathname, envelope: PumpEnvelope) {
    queues[pathname].push(envelope);
    wakers[pathname].shift()?.();
  }

  async function take(pathname: Pathname): Promise<PumpEnvelope | null> {
    const existing = queues[pathname].shift();
    if (existing) return existing;
    return new Promise((resolve) => {
      wakers[pathname].push(() => resolve(queues[pathname].shift() ?? null));
    });
  }

  async function dispatch(envelope: PumpEnvelope, pathname: Pathname): Promise<void> {
    const url = `${resolveBaseUrl(config)}/.well-known/workflow/v1/${pathname}`;
    const response = await fetch(url, {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        'x-vqs-queue-name': envelope.queueName,
        'x-vqs-message-id': envelope.messageId,
        'x-vqs-message-attempt': String(envelope.attempt),
      },
      body: stringify(envelope.message),
      signal: AbortSignal.timeout(httpTimeoutMs),
    });

    if (response.ok) {
      try {
        await response.body?.cancel();
      } catch {
        // The status already determines the queue outcome. Do not turn an ack
        // into a retry solely because releasing an unused body failed.
      }
      release(envelope);
      return;
    }

    const text = await response.text();

    if (response.status === 503) {
      let parsed: unknown;
      try {
        parsed = JSON.parse(text);
      } catch {
        parsed = null;
      }
      const timeoutSeconds = (parsed as { timeoutSeconds?: number } | null)?.timeoutSeconds;
      if (typeof timeoutSeconds === 'number') {
        // Same message re-delivered later: the idempotency key stays claimed.
        void delay(timeoutSeconds * 1000).then(() => enqueue(pathname, envelope));
        return;
      }
    }

    if (PERMANENT_ERROR_STATUSES.has(response.status)) {
      release(envelope);
      debug(
        `[world-celld test pump] dropping ${envelope.messageId}: permanent HTTP ${response.status}: ${text}`,
      );
      return;
    }

    if (envelope.attempt < maxAttempts) {
      const next: PumpEnvelope = { ...envelope, attempt: envelope.attempt + 1 };
      const backoff = baseBackoffMs * 2 ** (next.attempt - 1);
      void delay(backoff).then(() => enqueue(pathname, next));
    } else {
      release(envelope);
      console.error(
        `[world-celld test pump] dropping ${envelope.messageId} after ${envelope.attempt} attempts: HTTP ${response.status}: ${text}`,
      );
    }
  }

  async function loop(pathname: Pathname) {
    while (true) {
      if (!running) return;
      const envelope = await take(pathname);
      if (!envelope) continue;
      try {
        await dispatch(envelope, pathname);
      } catch (err) {
        release(envelope);
        console.error(`[world-celld test pump] dispatch error on ${pathname}:`, err);
      }
    }
  }

  return {
    /** Returns the inflight messageId when the key is already claimed. */
    inflight(idempotencyKey: string | undefined): MessageId | undefined {
      return idempotencyKey ? inflightMessages.get(idempotencyKey) : undefined;
    },
    push(pathname: Pathname, envelope: PumpEnvelope, delaySeconds?: number) {
      if (envelope.idempotencyKey) {
        inflightMessages.set(envelope.idempotencyKey, MessageId.parse(envelope.messageId));
      }
      if (delaySeconds && delaySeconds > 0) {
        void delay(delaySeconds * 1000).then(() => enqueue(pathname, envelope));
      } else {
        enqueue(pathname, envelope);
      }
    },
    async start() {
      if (running) return;
      running = true;
      void loop('flow');
    },
    stop() {
      running = false;
      for (const list of Object.values(wakers)) for (const w of list) w();
    },
  };
}

export function createQueue(config: CelldQueueConfig): Queue & { start(): Promise<void> } {
  const { env, deploymentId } = config;
  const queueShards = config.queueShards ?? 1;

  const generateMessageId = monotonicFactory();
  const testPump = createTestPump(config);

  const getQueueStub = (shard: number): QueueCellStub => {
    const id = env.WORKFLOW_QUEUE.idFromName(`q:${shard}`);
    return env.WORKFLOW_QUEUE.get(id);
  };

  return {
    async queue(queueName, message, opts) {
      parseQueueName(queueName);
      const runId =
        'runId' in message && typeof message.runId === 'string' ? message.runId : undefined;

      if (isTestMode()) {
        // Dedup on idempotencyKey while a message with the same key is in
        // flight — core re-enqueues every still-pending step on every replay
        // with idempotencyKey = stepId and relies on queue-level dedup.
        const existing = testPump.inflight(opts?.idempotencyKey);
        if (existing) {
          return { messageId: existing };
        }
        const messageId = MessageId.parse(`msg_${generateMessageId()}`);
        testPump.push(
          QUEUE_PATHNAME,
          {
            messageId,
            queueName,
            attempt: 1,
            message,
            idempotencyKey: opts?.idempotencyKey,
          },
          opts?.delaySeconds,
        );
        return { messageId };
      }

      // Production: enqueue into a QueueDO cell. The cell owns scheduling,
      // delivery, retries, dedup, and the dead-letter table.
      const messageId = MessageId.parse(`msg_${generateMessageId()}`);
      const shard = shardFor(opts?.idempotencyKey ?? messageId, queueShards);

      const outcome = await getQueueStub(shard).enqueue({
        messageId,
        queueName,
        pathname: QUEUE_PATHNAME,
        body: stringify(message),
        runId,
        idempotencyKey: opts?.idempotencyKey,
        delaySeconds: opts?.delaySeconds,
        config: {
          targetBaseUrl: resolveBaseUrl(config),
          queueShards,
        },
      });

      if (!outcome.ok) {
        if (outcome.code === 'RUN_EXPIRED') {
          throw new RunExpiredError(outcome.message);
        }
        throw new WorkflowWorldError(outcome.message, { status: 409 });
      }

      return { messageId: MessageId.parse(outcome.messageId) };
    },

    createQueueHandler(queueNamePrefix, handler) {
      // ONE dialect for the pump, QueueDO deliveries, and
      // @workflow/world-testing's mounted routes: x-vqs headers + tagged-JSON
      // body. The response taxonomy drives the sender's retry state machine:
      // - 2xx                     -> ack
      // - 503 + {timeoutSeconds}  -> redeliver after N seconds (key stays claimed)
      // - 404/409/410/422         -> permanent, drop without retrying
      // - other non-2xx           -> retry with capped backoff
      return async (req: Request) => {
        const reqQueueName = req.headers.get('x-vqs-queue-name');
        const reqMessageId = req.headers.get('x-vqs-message-id') as MessageId | null;
        const attemptStr = req.headers.get('x-vqs-message-attempt');

        if (!reqQueueName || !reqMessageId || !attemptStr || !req.body) {
          return Response.json({ error: 'Missing required headers or body' }, { status: 400 });
        }
        if (!reqQueueName.startsWith(queueNamePrefix)) {
          return Response.json({ error: 'Unhandled queue' }, { status: 400 });
        }
        const attempt = Number.parseInt(attemptStr, 10);
        try {
          // Tagged-JSON codec: revives Uint8Array payloads (runInput.input).
          const body = parse<unknown>(await req.text());
          const result = await handler(body, {
            attempt,
            queueName: reqQueueName,
            messageId: reqMessageId,
          });
          if (result && typeof result.timeoutSeconds === 'number') {
            return Response.json(
              { timeoutSeconds: result.timeoutSeconds },
              { status: 503, headers: { 'Retry-After': String(result.timeoutSeconds) } },
            );
          }
          return new Response(null, { status: 204 });
        } catch (error) {
          // Permanent vs transient distinction: permanent errors surface as
          // their own status so the sender drops instead of retrying.
          if (isPermanentError(error)) {
            debug('[world-celld queue handler] permanent error:', {
              status: error.status,
              error: String(error),
            });
            return Response.json(
              { error: String(error), permanent: true },
              { status: error.status },
            );
          }

          const backoffSeconds = computeBackoff(attempt);
          debug('[world-celld queue handler] transient error, will retry:', {
            error: String(error),
            backoffSeconds,
          });
          return Response.json(
            { error: String(error), retryAfter: backoffSeconds },
            { status: 500, headers: { 'Retry-After': String(backoffSeconds) } },
          );
        }
      };
    },

    async getDeploymentId() {
      return deploymentId;
    },

    async start() {
      if (isTestMode()) {
        await testPump.start();
      }
      // Production: QueueDO cells are push-based, nothing to start.
    },
  };
}
