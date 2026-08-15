import { DurableObject } from '../do-base.js';
import type { EnqueueOutcome, EnqueueRequest } from '../../queue.js';

/**
 * Queue cell: the celld replacement for Cloudflare Queues.
 *
 * One cell (`q:<shard>`) owns the full lifecycle of its messages: durable
 * scheduling via the cell alarm, delivery via outbound fetch (the same x-vqs
 * dialect as the in-process test pump), retries with capped exponential
 * backoff, idempotencyKey dedup, and a dead-letter table.
 *
 * Storage layout (KV-style, ordered scans via zero-padded epoch-ms keys):
 *   cfg                          pinned QueueCellConfig (first-enqueue wins)
 *   msg:<messageId>              MessageRow
 *   due:<paddedMs>:<messageId>   schedule index -> messageId
 *   inflight:<messageId>         crash-recovery deadline (epoch ms)
 *   key:<idempotencyKey>         active dedup claim -> messageId
 *   dlq:<paddedMs>:<messageId>   DeadLetterRow
 *
 * celld gotchas designed around:
 * - celld#144 (alarm handlers can overlap): the storage-only claim phase runs
 *   inside blockConcurrencyWhile; slow delivery I/O runs outside the gate via
 *   waitUntil.
 * - six-failure alarm abandonment: the alarm handler never throws for data
 *   conditions — backoff reschedules the alarm instead. rearmAlarm() is the
 *   recovery hatch, and every enqueue also re-arms.
 * - deploys restart the fleet: in-flight deliveries die with the isolate; the
 *   inflight-deadline sweep on the next alarm redelivers (at-least-once).
 */

export interface MessageRow {
  messageId: string;
  queueName: string;
  pathname: 'flow' | 'step';
  /** Pre-encoded tagged-JSON payload — forwarded byte-identical. */
  body: string;
  /**
   * Delivery target captured at enqueue time. Carried per message (not
   * pinned on the cell) so an app redeploy at a new URL never bricks the
   * queue: old messages deliver (or dead-letter) against their original
   * target, new messages against the new one.
   */
  targetBaseUrl: string;
  idempotencyKey?: string;
  /** Completed delivery attempts. */
  attempt: number;
  enqueuedAt: number;
  lastError?: string;
}

export interface DeadLetterRow extends MessageRow {
  failedAt: number;
}

export interface QueueStats {
  pending: number;
  inflight: number;
  deadLetters: number;
  alarmAt: number | null;
}

const PERMANENT_STATUSES = new Set([404, 409, 410, 422]);

const DEFAULT_MAX_ATTEMPTS = 5;
const DEFAULT_MAX_INFLIGHT = 5;
const DEFAULT_DELIVERY_TIMEOUT_MS = 300_000;
/** Grace period past the delivery timeout before a claim counts as lost. */
const INFLIGHT_GRACE_MS = 30_000;

function pad(ms: number): string {
  return String(Math.max(0, Math.floor(ms))).padStart(13, '0');
}

function dueKey(atMs: number, messageId: string): string {
  return `due:${pad(atMs)}:${messageId}`;
}

function backoffSeconds(attempt: number): number {
  return Math.min(60, 2 ** attempt);
}

interface QueueCellEnv {
  QUEUE_MAX_ATTEMPTS?: string;
  QUEUE_MAX_INFLIGHT?: string;
  QUEUE_DELIVERY_TIMEOUT_MS?: string;
  WORKFLOW_CALLBACK_SECRET?: string;
  /** Test seams (never set by a real deploy — celld vars are strings). */
  clock?: () => number;
  fetch?: typeof fetch;
}

export class QueueDO extends DurableObject {
  #now(): number {
    const clock = (this.env as QueueCellEnv)?.clock;
    return typeof clock === 'function' ? clock() : Date.now();
  }

  #fetch(): typeof fetch {
    const impl = (this.env as QueueCellEnv)?.fetch;
    return typeof impl === 'function' ? impl : fetch;
  }

  #intVar(name: 'QUEUE_MAX_ATTEMPTS' | 'QUEUE_MAX_INFLIGHT' | 'QUEUE_DELIVERY_TIMEOUT_MS', fallback: number): number {
    const raw = (this.env as QueueCellEnv)?.[name];
    const parsed = raw ? Number.parseInt(raw, 10) : Number.NaN;
    return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
  }

  async enqueue(request: EnqueueRequest): Promise<EnqueueOutcome> {
    const storage = this.ctx.storage;
    const now = this.#now();

    // Pin the shard count from the first enqueue and reject drift loudly:
    // idempotencyKey -> cell affinity (and therefore dedup correctness)
    // depends on every producer agreeing on the shard count. The delivery
    // URL deliberately is NOT pinned — it travels per message.
    const pinned = await storage.get<{ queueShards: number; pinnedAt: number }>('cfg');
    if (!pinned) {
      await storage.put('cfg', { queueShards: request.config.queueShards, pinnedAt: now });
    } else if (pinned.queueShards !== request.config.queueShards) {
      return {
        ok: false,
        code: 'CONFIG_MISMATCH',
        message:
          `queue cell shard-count mismatch: pinned ${pinned.queueShards}, ` +
          `got ${request.config.queueShards} — changing queueShards requires draining the fleet`,
      };
    }

    // Dedup: while a message with this idempotencyKey is active, return the
    // original messageId (core re-enqueues pending steps on every replay and
    // relies on this).
    if (request.idempotencyKey) {
      const existing = await storage.get<string>(`key:${request.idempotencyKey}`);
      if (existing) {
        if (await storage.get(`msg:${existing}`)) {
          return { ok: true, messageId: existing, deduped: true };
        }
        // Stale claim (message gone without release) — steal it.
        await storage.delete(`key:${request.idempotencyKey}`);
      }
      await storage.put(`key:${request.idempotencyKey}`, request.messageId);
    }

    const row: MessageRow = {
      messageId: request.messageId,
      queueName: request.queueName,
      pathname: request.pathname,
      body: request.body,
      targetBaseUrl: request.config.targetBaseUrl,
      idempotencyKey: request.idempotencyKey,
      attempt: 0,
      enqueuedAt: now,
    };
    const dueAt = now + Math.max(0, request.delaySeconds ?? 0) * 1000;
    await storage.put(`msg:${row.messageId}`, row);
    await storage.put(dueKey(dueAt, row.messageId), row.messageId);

    await this.#armAlarmAtMost(dueAt);

    return { ok: true, messageId: row.messageId, deduped: false };
  }

  async alarm(): Promise<void> {
    // celld#144: alarm handlers can overlap while awaiting. The storage-only
    // claim phase runs under the input gate; a failure is carried out and
    // rethrown outside so a failed critical section cannot reset the cell.
    let claimed: MessageRow[] = [];
    let failure: unknown = null;
    const attempt = async () => {
      try {
        claimed = await this.#claimPhase();
      } catch (error) {
        failure = error;
      }
    };
    if (typeof this.ctx.blockConcurrencyWhile === 'function') {
      await this.ctx.blockConcurrencyWhile(attempt);
    } else {
      await attempt();
    }
    if (failure !== null) throw failure;

    for (const row of claimed) {
      this.ctx.waitUntil(this.#deliver(row));
    }
  }

  /**
   * Storage-only: recover lost inflight claims, claim due messages up to the
   * inflight cap, and re-arm the alarm. Never throws for data conditions.
   */
  async #claimPhase(): Promise<MessageRow[]> {
    const storage = this.ctx.storage;
    const now = this.#now();
    const maxInflight = this.#intVar('QUEUE_MAX_INFLIGHT', DEFAULT_MAX_INFLIGHT);

    // 1. Recover: an inflight entry past its deadline is a lost delivery
    // (node crash, deploy restart) — back to due for redelivery.
    const inflight = await storage.list<number>({ prefix: 'inflight:' });
    let inflightCount = 0;
    for (const [key, deadline] of inflight) {
      if (deadline <= now) {
        const messageId = key.slice('inflight:'.length);
        await storage.delete(key);
        if (await storage.get(`msg:${messageId}`)) {
          await storage.put(dueKey(now, messageId), messageId);
        }
      } else {
        inflightCount++;
      }
    }

    // 2. Claim due messages up to the cap.
    const claimed: MessageRow[] = [];
    if (inflightCount < maxInflight) {
      const due = await storage.list<string>({
        prefix: 'due:',
        end: `due:${pad(now + 1)}`,
        limit: maxInflight - inflightCount,
      });
      const timeoutMs = this.#intVar('QUEUE_DELIVERY_TIMEOUT_MS', DEFAULT_DELIVERY_TIMEOUT_MS);
      for (const [key, messageId] of due) {
        await storage.delete(key);
        const row = await storage.get<MessageRow>(`msg:${messageId}`);
        if (!row) continue; // orphaned schedule entry
        await storage.put(`inflight:${messageId}`, now + timeoutMs + INFLIGHT_GRACE_MS);
        claimed.push(row);
      }
    }

    await this.#rearm();
    return claimed;
  }

  async #deliver(row: MessageRow): Promise<void> {
    const storage = this.ctx.storage;
    const timeoutMs = this.#intVar('QUEUE_DELIVERY_TIMEOUT_MS', DEFAULT_DELIVERY_TIMEOUT_MS);
    const callbackSecret = (this.env as QueueCellEnv)?.WORKFLOW_CALLBACK_SECRET;
    const url = `${row.targetBaseUrl.replace(/\/$/, '')}/.well-known/workflow/v1/${row.pathname}`;

    let response: Response | null = null;
    let transportError: unknown = null;
    try {
      response = await this.#fetch()(url, {
        method: 'POST',
        headers: {
          'content-type': 'application/json',
          'x-vqs-queue-name': row.queueName,
          'x-vqs-message-id': row.messageId,
          'x-vqs-message-attempt': String(row.attempt + 1),
          ...(callbackSecret ? { 'x-workflow-callback-secret': callbackSecret } : {}),
        },
        body: row.body,
        signal: AbortSignal.timeout(timeoutMs),
      });
    } catch (error) {
      transportError = error;
    }

    const now = this.#now();

    if (response?.ok) {
      // Ack: the message is done; release the dedup claim.
      await this.#ack(row);
    } else if (response && response.status === 503) {
      // {timeoutSeconds} means "redeliver later, same message": the attempt
      // count does not advance and the dedup key stays claimed.
      let timeoutSeconds: number | undefined;
      try {
        const parsed = (await response.json()) as { timeoutSeconds?: number };
        timeoutSeconds = parsed?.timeoutSeconds;
      } catch {
        timeoutSeconds = undefined;
      }
      if (typeof timeoutSeconds === 'number') {
        await storage.delete(`inflight:${row.messageId}`);
        await storage.put(dueKey(now + timeoutSeconds * 1000, row.messageId), row.messageId);
      } else {
        await this.#retry(row, 'HTTP 503');
      }
    } else if (response && PERMANENT_STATUSES.has(response.status)) {
      // Permanent: drop without burning retries; the handler already
      // classified this as unreplayable.
      await this.#ack(row);
    } else {
      const reason = response
        ? `HTTP ${response.status}`
        : `transport error: ${String(transportError)}`;
      await this.#retry(row, reason);
    }

    await this.#rearm();
  }

  async #ack(row: MessageRow): Promise<void> {
    const storage = this.ctx.storage;
    await storage.delete(`msg:${row.messageId}`);
    await storage.delete(`inflight:${row.messageId}`);
    if (row.idempotencyKey) {
      const holder = await storage.get<string>(`key:${row.idempotencyKey}`);
      if (holder === row.messageId) {
        await storage.delete(`key:${row.idempotencyKey}`);
      }
    }
  }

  async #retry(row: MessageRow, reason: string): Promise<void> {
    const storage = this.ctx.storage;
    const now = this.#now();
    const maxAttempts = this.#intVar('QUEUE_MAX_ATTEMPTS', DEFAULT_MAX_ATTEMPTS);
    const attempt = row.attempt + 1;

    await storage.delete(`inflight:${row.messageId}`);

    if (attempt >= maxAttempts) {
      const dead: DeadLetterRow = { ...row, attempt, lastError: reason, failedAt: now };
      await storage.put(`dlq:${pad(now)}:${row.messageId}`, dead);
      await storage.delete(`msg:${row.messageId}`);
      if (row.idempotencyKey) {
        const holder = await storage.get<string>(`key:${row.idempotencyKey}`);
        if (holder === row.messageId) {
          await storage.delete(`key:${row.idempotencyKey}`);
        }
      }
      return;
    }

    const updated: MessageRow = { ...row, attempt, lastError: reason };
    await storage.put(`msg:${row.messageId}`, updated);
    await storage.put(dueKey(now + backoffSeconds(attempt) * 1000, row.messageId), row.messageId);
  }

  /** Arm the alarm no later than `atMs` (durable backoff never throws). */
  async #armAlarmAtMost(atMs: number): Promise<void> {
    const current = await this.ctx.storage.getAlarm();
    if (current === null || atMs < current) {
      await this.ctx.storage.setAlarm(atMs);
    }
  }

  /** Recompute the alarm from the earliest due entry / inflight deadline. */
  async #rearm(): Promise<void> {
    const storage = this.ctx.storage;
    let next: number | null = null;

    const due = await storage.list<string>({ prefix: 'due:', limit: 1 });
    for (const key of due.keys()) {
      next = Number.parseInt(key.slice('due:'.length, 'due:'.length + 13), 10);
    }

    const inflight = await storage.list<number>({ prefix: 'inflight:' });
    for (const deadline of inflight.values()) {
      if (next === null || deadline < next) next = deadline;
    }

    if (next !== null) {
      await storage.setAlarm(next);
    } else {
      await storage.deleteAlarm();
    }
  }

  // ---- Admin RPC (exposed through the router's queue binding) ----

  async stats(): Promise<QueueStats> {
    const storage = this.ctx.storage;
    const [due, inflight, dlq, alarmAt] = await Promise.all([
      storage.list({ prefix: 'due:' }),
      storage.list({ prefix: 'inflight:' }),
      storage.list({ prefix: 'dlq:' }),
      storage.getAlarm(),
    ]);
    return {
      pending: due.size,
      inflight: inflight.size,
      deadLetters: dlq.size,
      alarmAt,
    };
  }

  async listDeadLetters(params?: {
    limit?: number;
    cursor?: string;
  }): Promise<{ data: DeadLetterRow[]; cursor: string | null; hasMore: boolean }> {
    const limit = params?.limit ?? 100;
    const entries = await this.ctx.storage.list<DeadLetterRow>({
      prefix: 'dlq:',
      startAfter: params?.cursor,
      limit: limit + 1,
    });
    const keys = Array.from(entries.keys());
    const page = keys.slice(0, limit);
    const hasMore = keys.length > limit;
    return {
      data: page.map((k) => entries.get(k) as DeadLetterRow),
      cursor: hasMore ? page.at(-1)! : null,
      hasMore,
    };
  }

  /** Move a dead letter back to the live queue (attempt count reset). */
  async redriveDeadLetter(messageId: string): Promise<{ ok: boolean }> {
    const storage = this.ctx.storage;
    const dlq = await storage.list<DeadLetterRow>({ prefix: 'dlq:' });
    for (const [key, dead] of dlq) {
      if (dead.messageId !== messageId) continue;
      const now = this.#now();
      const { failedAt: _failedAt, ...rest } = dead;
      const row: MessageRow = { ...rest, attempt: 0, lastError: undefined };
      await storage.put(`msg:${row.messageId}`, row);
      await storage.put(dueKey(now, row.messageId), row.messageId);
      if (row.idempotencyKey && !(await storage.get(`key:${row.idempotencyKey}`))) {
        await storage.put(`key:${row.idempotencyKey}`, row.messageId);
      }
      await storage.delete(key);
      await this.#armAlarmAtMost(now);
      return { ok: true };
    }
    return { ok: false };
  }

  async purgeDeadLetters(): Promise<{ purged: number }> {
    const storage = this.ctx.storage;
    const dlq = await storage.list({ prefix: 'dlq:' });
    for (const key of dlq.keys()) {
      await storage.delete(key);
    }
    return { purged: dlq.size };
  }

  /**
   * Recovery hatch for celld's six-failure alarm abandonment: derive the due
   * time again and arm it.
   */
  async rearmAlarm(): Promise<{ alarmAt: number | null }> {
    await this.#rearm();
    return { alarmAt: await this.ctx.storage.getAlarm() };
  }
}
