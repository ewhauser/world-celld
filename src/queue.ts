/**
 * Queue implementation for the celld world.
 *
 * Vendored from vinnymac/worlds packages/world-cloudflare/src/queue.ts
 * (Apache-2.0, see NOTICE), modified for celld:
 *
 * - Production publishes a small pointer through celld v0.4.0 native Queues.
 *   The deployed Queue consumer forwards deliveries to the same HTTP handler
 *   used by the in-process test pump.
 * - Run-bearing message bodies live in the worker's R2 binding. This preserves
 *   the World's larger payload contract and lets run retention delete payload
 *   bytes independently of the broker's fixed retention window.
 * - `createQueueHandler` has ONE dialect — the x-vqs wire format the test
 *   pump has always used — extended so permanent errors surface as their
 *   HTTP statuses. Native Queue deliveries and the in-process test pump hit the
 *   exact same handler path.
 */
import { WorkflowWorldError } from '@workflow/errors';
import {
  MessageId,
  parseQueueName,
  QueuePayloadSchema,
  type Queue,
  type QueuePayload,
  type ValidQueueName,
} from '@workflow/world';
import { parse, stringify } from './vendor/shared/index.js';
import { monotonicFactory } from 'ulid';
import { debug } from './util.js';
import {
  nativeQueueDelaySeconds,
  type NativeQueueEnvelope,
  type NativeQueueSendOptions,
  type NativeQueueSendResult,
} from './queue-protocol.js';
import {
  MAX_QUEUE_DELIVERY_TIMEOUT_MS,
  MAX_QUEUE_DERIVED_DEADLINE_HEADROOM_MS,
} from './lifecycle.js';
import {
  boundedIntegerOption,
  checkedQueueTimestampAdd,
  queueDelayDeadline,
  strictIntegerSetting,
} from './validation.js';

const MAX_TIMER_DELAY_MS = 0x7fffffff;
const MAX_TEST_PUMP_BACKOFF_MS = 60_000;

function delay(milliseconds: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

async function delayFor(milliseconds: number): Promise<void> {
  let remaining = milliseconds;
  while (remaining > MAX_TIMER_DELAY_MS) {
    await delay(MAX_TIMER_DELAY_MS);
    remaining -= MAX_TIMER_DELAY_MS;
  }
  await delay(remaining);
}

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
  // Explicit override: force the production native-Queue path even under a test
  // runner (used by the conformance suite to exercise the live native queue —
  // the world-testing server inherits VITEST from the vitest parent).
  if (process.env.CELLD_QUEUE_MODE === 'native') return false;
  return process.env.VITEST === 'true' || process.env.NODE_ENV === 'test';
}

export interface CelldQueueProducer {
  send(
    envelope: NativeQueueEnvelope,
    options?: NativeQueueSendOptions,
  ): Promise<NativeQueueSendResult>;
}

export interface CelldQueueConfig {
  env: {
    WORKFLOW_QUEUE: CelldQueueProducer;
  };
  deploymentId: string;
  /**
   * Base URL the app's workflow endpoints are mounted on. The native Queue
   * consumer delivers to `${baseUrl}/.well-known/workflow/v1/flow`; the test
   * pump uses the same value.
   * Default: process.env.WORKFLOW_BASE_URL || `http://localhost:${process.env.PORT ?? 3000}`
   */
  baseUrl?: string;
  /** Per-job HTTP request timeout (ms) for the test pump. Default/max: 300_000 */
  httpTimeoutMs?: number;
  /** Maximum retry attempts in the test pump before dropping a job. Default: 5 */
  maxAttempts?: number;
  /** Base backoff delay (ms) for test pump retries. Default: 1000; maximum: 60_000 */
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
 * In-process test pump. Holds an in-memory FIFO and HTTP-dispatches envelopes
 * to the user's server at `${baseUrl}/.well-known/workflow/v1/flow`.
 *
 * Mirrors world-local's idempotency semantics: messages are deduplicated on
 * `idempotencyKey` while a message with the same key is in flight, and the
 * key is released only when the message is fully handled (success or drop).
 *
 * Modified vs upstream: permanent-error statuses (404/409/410/422) from the
 * handler drop the message immediately instead of burning retry budget —
 * matching the native Queue consumer's production semantics.
 */
function createTestPump(config: CelldQueueConfig) {
  const httpTimeoutMs = boundedIntegerOption(
    'world-celld test pump httpTimeoutMs',
    config.httpTimeoutMs,
    300_000,
    1,
    MAX_QUEUE_DELIVERY_TIMEOUT_MS,
  );
  const maxAttempts = boundedIntegerOption(
    'world-celld test pump maxAttempts',
    config.maxAttempts,
    5,
    1,
  );
  const baseBackoffMs = boundedIntegerOption(
    'world-celld test pump backoffDelayMs',
    config.backoffDelayMs,
    1000,
    0,
    MAX_TEST_PUMP_BACKOFF_MS,
  );

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
      const now = Date.now();
      const redeliveryAt = queueDelayDeadline(
        now,
        timeoutSeconds,
        1,
        MAX_QUEUE_DERIVED_DEADLINE_HEADROOM_MS,
      );
      if (redeliveryAt !== null) {
        // Same message re-delivered later: the idempotency key stays claimed.
        void delayFor(redeliveryAt - now).then(() => enqueue(pathname, envelope));
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
      const backoff = Math.min(
        MAX_TEST_PUMP_BACKOFF_MS,
        baseBackoffMs * 2 ** Math.min(next.attempt - 1, 30),
      );
      if (
        checkedQueueTimestampAdd(Date.now(), backoff, MAX_QUEUE_DERIVED_DEADLINE_HEADROOM_MS) ===
        null
      ) {
        release(envelope);
        console.error(
          `[world-celld test pump] dropping ${envelope.messageId}: retry deadline exceeds the queue timestamp horizon`,
        );
        return;
      }
      void delayFor(backoff).then(() => enqueue(pathname, next));
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
      if (
        queueDelayDeadline(
          Date.now(),
          delaySeconds ?? 0,
          0,
          MAX_QUEUE_DERIVED_DEADLINE_HEADROOM_MS,
        ) === null
      ) {
        throw new Error('world-celld queue delaySeconds lacks required delivery headroom');
      }
      if (envelope.idempotencyKey) {
        inflightMessages.set(envelope.idempotencyKey, MessageId.parse(envelope.messageId));
      }
      if (delaySeconds && delaySeconds > 0) {
        void delayFor(delaySeconds * 1000).then(() => enqueue(pathname, envelope));
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
  if (!env?.WORKFLOW_QUEUE) {
    throw new Error('world-celld queue missing WORKFLOW_QUEUE binding');
  }
  const generateMessageId = monotonicFactory();
  const testPump = createTestPump(config);

  return {
    async queue(queueName, message, opts) {
      parseQueueName(queueName);
      const parsedMessage = QueuePayloadSchema.safeParse(message);
      if (!parsedMessage.success) {
        throw new WorkflowWorldError('world-celld queue payload is invalid', { status: 422 });
      }
      if (
        queueDelayDeadline(
          Date.now(),
          opts?.delaySeconds ?? 0,
          0,
          MAX_QUEUE_DERIVED_DEADLINE_HEADROOM_MS,
        ) === null
      ) {
        throw new Error('world-celld queue delaySeconds lacks required delivery headroom');
      }
      const runId =
        'runId' in parsedMessage.data && typeof parsedMessage.data.runId === 'string'
          ? parsedMessage.data.runId
          : undefined;

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
            message: parsedMessage.data,
            idempotencyKey: opts?.idempotencyKey,
          },
          opts?.delaySeconds,
        );
        return { messageId };
      }

      // Production: publish through celld's native Queue. The worker-side send
      // route moves run-bearing payload bytes into R2 before it publishes this
      // envelope, so only a bounded pointer reaches the broker.
      const messageId = MessageId.parse(`msg_${generateMessageId()}`);
      const body = stringify(parsedMessage.data);
      const notBefore =
        opts?.delaySeconds && opts.delaySeconds > 0
          ? (queueDelayDeadline(
              Date.now(),
              opts.delaySeconds,
              0,
              MAX_QUEUE_DERIVED_DEADLINE_HEADROOM_MS,
            ) ?? undefined)
          : undefined;
      const envelope: NativeQueueEnvelope = {
        version: 1,
        messageId,
        queueName,
        targetBaseUrl: resolveBaseUrl(config),
        runId,
        idempotencyKey: opts?.idempotencyKey,
        body,
        notBefore,
      };
      const published = await env.WORKFLOW_QUEUE.send(envelope, {
        delaySeconds: nativeQueueDelaySeconds(notBefore, Date.now()),
      });
      return { messageId: MessageId.parse(published.messageId) };
    },

    createQueueHandler(queueNamePrefix, handler) {
      // ONE dialect for the pump, native Queue consumer deliveries, and
      // @workflow/world-testing's mounted routes: x-vqs headers + tagged-JSON
      // body. The response taxonomy drives the sender's retry state machine:
      // - 2xx                     -> ack
      // - 503 + {timeoutSeconds}  -> redeliver after N seconds (key stays claimed)
      // - 404/409/410/422         -> permanent, drop without retrying
      // - other non-2xx           -> retry with capped backoff
      return async (req: Request) => {
        if (req.method !== 'POST') {
          return Response.json({ error: 'Expected POST' }, { status: 405 });
        }
        if (
          req.headers.get('content-type')?.split(';', 1)[0].trim().toLowerCase() !==
          'application/json'
        ) {
          return Response.json({ error: 'Expected application/json' }, { status: 415 });
        }
        const reqQueueName = req.headers.get('x-vqs-queue-name');
        const reqMessageId = req.headers.get('x-vqs-message-id') as MessageId | null;
        const attemptStr = req.headers.get('x-vqs-message-attempt');

        if (!reqQueueName || !reqMessageId || !attemptStr || !req.body) {
          return Response.json({ error: 'Missing required headers or body' }, { status: 400 });
        }
        if (!reqQueueName.startsWith(queueNamePrefix)) {
          return Response.json({ error: 'Unhandled queue' }, { status: 400 });
        }
        let attempt: number;
        try {
          attempt = strictIntegerSetting('x-vqs-message-attempt', attemptStr, 1, 1);
        } catch {
          return Response.json({ error: 'Invalid x-vqs-message-attempt header' }, { status: 400 });
        }
        try {
          // Tagged-JSON codec: revives Uint8Array payloads (runInput.input).
          let body: unknown;
          try {
            body = parse<unknown>(await req.text());
          } catch {
            return Response.json({ error: 'Malformed tagged JSON body' }, { status: 422 });
          }
          const parsedBody = QueuePayloadSchema.safeParse(body);
          if (!parsedBody.success) {
            return Response.json({ error: 'Invalid queue payload' }, { status: 422 });
          }
          const result = await handler(parsedBody.data, {
            attempt,
            queueName: reqQueueName,
            messageId: reqMessageId,
          });
          if (result && typeof result.timeoutSeconds === 'number') {
            if (
              queueDelayDeadline(
                Date.now(),
                result.timeoutSeconds,
                1,
                MAX_QUEUE_DERIVED_DEADLINE_HEADROOM_MS,
              ) === null
            ) {
              throw new Error('Queue handler timeoutSeconds is out of range');
            }
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
      // Production: celld native Queues are push-based, nothing to start.
    },
  };
}
